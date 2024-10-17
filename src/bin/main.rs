use anyhow::{anyhow, Context as _};
use futures_util::{future::select_all, StreamExt as _};
use prost::Message as _;
use rdkafka::{
    consumer::{Consumer as _, DefaultConsumerContext, MessageStream, StreamConsumer},
    producer::FutureProducer,
    Message as _,
};
use serde::Deserialize;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use titorelli::print_unix_millis;
use tokio::{sync::mpsc, task::JoinHandle};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

const AGGREGATION_INTERVAL_MINUTES: u32 = 60;
const AGGREGATION_INTERVAL_MILLIS: i64 = AGGREGATION_INTERVAL_MINUTES as i64 * 60000;

#[derive(Deserialize)]
struct Config {
    kafka: BTreeMap<String, String>,
}

#[tokio::main]
async fn main() {
    init_tracing();
    match run().await {
        Ok(()) => tracing::error!("exit"),
        Err(err) => tracing::error!("{:#}", err),
    }
}

fn init_tracing() {
    use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};
    let env_layer = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::registry()
        .with(env_layer)
        .with(tracing_subscriber::fmt::layer())
        .init();
}

async fn run() -> anyhow::Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow!("missing config path (1st arg)"))?;
    let config = std::fs::read_to_string(config_path).context("read config")?;
    let config: Config = serde_json::from_str(&config).context("parse config")?;

    let consumer: rdkafka::consumer::StreamConsumer = titorelli::kafka::config_with(
        config.kafka.clone(),
        [
            ("enable.auto.offset.store", "true"),
            ("enable.auto.commit", "true"),
        ],
        &[],
    )?
    .create()?;
    let consumer = Arc::new(consumer);
    let producer: rdkafka::producer::FutureProducer = titorelli::kafka::config_with(
        config.kafka.clone(),
        [("compression.codec", "gzip")],
        &["group.id"],
    )?
    .create()?;

    let start_timestamp = latest_sink_timestamp(&consumer).await?.unwrap_or(0);
    consumer.unassign().context("unassign sinks")?;
    tracing::info!(start_timestamp = print_unix_millis(start_timestamp)?);

    let assignment =
        titorelli::kafka::assign_partitions(&consumer, &[SOURCE_TOPIC], start_timestamp).await?;

    let (source_msg_tx, mut source_msg_rx) = mpsc::channel::<SourceMsg>(1024);
    let mut partition_consumers: Vec<JoinHandle<()>> = assignment
        .elements()
        .into_iter()
        .map(|e| {
            spawn_partition_consumer(&consumer, e.topic(), e.partition(), source_msg_tx.clone())
        })
        .collect();
    drop(source_msg_tx);

    let msg_buffer_limit = 128;
    let mut msg_buffer: Vec<SourceMsg> = Vec::with_capacity(msg_buffer_limit);
    let mut aggregations: BTreeMap<i64, Aggregations> = Default::default();
    let mut flush_timestamps: BTreeMap<String, i64> = assignment
        .elements()
        .into_iter()
        .map(|e| (format!("{}/{}", e.topic(), e.partition()), 0))
        .collect();
    loop {
        tokio::select! {
            biased;
            _ = consumer.recv() => anyhow::bail!("message received from split consumer"),
            _ = select_all(&mut partition_consumers) => anyhow::bail!("parition consumer exit"),
            _ = source_msg_rx.recv_many(&mut msg_buffer, msg_buffer_limit) => {
                anyhow::ensure!(!msg_buffer.is_empty(), "consumer channel closed");
                for msg in msg_buffer.drain(..) {
                    match msg {
                        SourceMsg::Flush { partition_id, aggregation_timestamp } => {
                            *flush_timestamps.get_mut(&partition_id).unwrap() = aggregation_timestamp;
                            let min_timestamp = flush_timestamps.values().cloned().min().unwrap_or(0);
                            while aggregations.first_key_value().map(|(k, _)| *k <= min_timestamp).unwrap_or(false) {
                                let (t, agg) = aggregations.pop_first().unwrap();
                                record_aggregations(&producer, t, agg).await?;
                                tracing::info!(timestamp = print_unix_millis(t).unwrap(), "flushed");
                            }
                        }
                        SourceMsg::IndexerFees {
                            aggregation_timestamp,
                            signer,
                            receiver,
                            fees_grt,
                        } => {
                            let key = IndexerFeesKey { signer, receiver };
                            let agg = aggregations.entry(aggregation_timestamp).or_default();
                            *agg.indexer_fees.entry(key).or_default() += fees_grt;
                        }
                    }
                }
            }
        };
    }
}

async fn latest_sink_timestamp(consumer: &StreamConsumer) -> anyhow::Result<Option<i64>> {
    let latest_messages = titorelli::kafka::latest_messages(consumer, &[SINK_TOPIC]).await?;
    let timestamp = latest_messages
        .into_iter()
        .map(|msg| -> anyhow::Result<i64> {
            let msg = IndexerFeesHourlyProtobuf::decode(msg.payload().context("missing payload")?)?;
            Ok(msg.timestamp)
        })
        .collect::<anyhow::Result<Vec<i64>>>()?
        .into_iter()
        .max();
    Ok(timestamp)
}

fn spawn_partition_consumer(
    consumer: &Arc<StreamConsumer>,
    topic: &str,
    partition: i32,
    source_msg_tx: mpsc::Sender<SourceMsg>,
) -> JoinHandle<()> {
    let id = format!("{}/{}", topic, partition);
    let partition_queue = consumer.split_partition_queue(topic, partition).unwrap();
    async fn handle_stream(
        id: String,
        mut stream: MessageStream<'_, DefaultConsumerContext>,
        source_msg_tx: mpsc::Sender<SourceMsg>,
    ) -> anyhow::Result<()> {
        let mut last_aggregation_timestamp = 0;
        loop {
            let msg = stream.next().await.context("stream closed")??;
            let msg = SourceMsg::decode(msg)?;
            let aggregation_timestamp = match &msg {
                SourceMsg::Flush { .. } => unreachable!(),
                SourceMsg::IndexerFees {
                    aggregation_timestamp,
                    ..
                } => *aggregation_timestamp,
            };
            let _ = source_msg_tx.send(msg).await;
            if aggregation_timestamp > last_aggregation_timestamp {
                // add a grace period to allow straggling messages to come in before flushing
                let flush_msg = SourceMsg::Flush {
                    partition_id: id.clone(),
                    aggregation_timestamp: last_aggregation_timestamp,
                };
                let source_msg_tx = source_msg_tx.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    let _ = source_msg_tx.send(flush_msg).await;
                });

                last_aggregation_timestamp = aggregation_timestamp;
            }
        }
    }
    tokio::spawn(async move {
        let stream = partition_queue.stream();
        if let Err(err) = handle_stream(id.clone(), stream, source_msg_tx).await {
            tracing::error!("{:#}", err.context(id));
        }
    })
}

async fn record_aggregations(
    producer: &FutureProducer,
    timestamp: i64,
    aggregations: Aggregations,
) -> anyhow::Result<()> {
    let Aggregations { indexer_fees } = aggregations;
    let record_payload = IndexerFeesHourlyProtobuf {
        timestamp,
        aggregations: indexer_fees
            .into_iter()
            .map(|(k, v)| IndexerFeesProtobuf {
                signer: k.signer.0.into(),
                receiver: k.receiver.0.into(),
                fees_grt: v,
            })
            .collect(),
    }
    .encode_to_vec();
    let key = timestamp.to_be_bytes();
    let record = rdkafka::producer::FutureRecord::to(SINK_TOPIC)
        .key(&key)
        .payload(&record_payload);
    producer
        .send(record, Duration::from_secs(30))
        .await
        .map_err(|(err, _)| err)
        .context("send aggregation record")?;

    Ok(())
}

const SOURCE_TOPIC: &str = "gateway_indexer_fees";
const SINK_TOPIC: &str = "gateway_indexer_fees_hourly";

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Address([u8; 20]);

impl Address {
    fn from_slice(input: &[u8]) -> anyhow::Result<Self> {
        let mut output = [0_u8; 20];
        anyhow::ensure!(
            input.len() == output.len(),
            "invalid address len: {}",
            input.len()
        );
        output.copy_from_slice(input);
        Ok(Self(output))
    }
}

impl std::fmt::Debug for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x")?;
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Debug)]
enum SourceMsg {
    Flush {
        partition_id: String,
        aggregation_timestamp: i64,
    },
    IndexerFees {
        aggregation_timestamp: i64,
        signer: Address,
        receiver: Address,
        fees_grt: f64,
    },
}

#[derive(prost::Message)]
struct IndexerFeesProtobuf {
    /// 20 bytes (address)
    #[prost(bytes, tag = "1")]
    signer: Vec<u8>,
    /// 20 bytes (address)
    #[prost(bytes, tag = "2")]
    receiver: Vec<u8>,
    #[prost(double, tag = "3")]
    fees_grt: f64,
}

impl SourceMsg {
    fn decode(msg: rdkafka::message::BorrowedMessage) -> anyhow::Result<Self> {
        anyhow::ensure!(msg.topic() == SOURCE_TOPIC);
        let timestamp = msg
            .timestamp()
            .to_millis()
            .ok_or_else(|| anyhow!("missing timestamp"))?;
        let payload = msg.payload().context("missing payload")?;
        let decoded = IndexerFeesProtobuf::decode(payload).context("decode protobuf")?;
        let aggregation_timestamp = timestamp - (timestamp % AGGREGATION_INTERVAL_MILLIS);
        Ok(SourceMsg::IndexerFees {
            aggregation_timestamp,
            signer: Address::from_slice(&decoded.signer)?,
            receiver: Address::from_slice(&decoded.receiver)?,
            fees_grt: decoded.fees_grt,
        })
    }
}

#[derive(Debug, Default)]
struct Aggregations {
    indexer_fees: BTreeMap<IndexerFeesKey, f64>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct IndexerFeesKey {
    signer: Address,
    receiver: Address,
}

#[derive(prost::Message)]
struct IndexerFeesHourlyProtobuf {
    /// start timestamp for aggregation, in unix milliseconds
    #[prost(int64, tag = "1")]
    timestamp: i64,
    #[prost(message, repeated, tag = "2")]
    aggregations: Vec<IndexerFeesProtobuf>,
}
