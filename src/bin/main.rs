use anyhow::{anyhow, Context as _};
use futures_util::{future::select_all, StreamExt as _};
use prost::Message as _;
use rdkafka::{
    consumer::{Consumer as _, DefaultConsumerContext, MessageStream, StreamConsumer},
    producer::{DefaultProducerContext, FutureProducer, ThreadedProducer},
    Message as _, TopicPartitionList,
};
use serde::Deserialize;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use titorelli::{
    kafka::{assign_partitions, fetch_partition_ids, latest_messages},
    messages::{
        ClientFeesHourlyProtobuf, ClientFeesProtobuf, ClientQueryProtobuf,
        IndexerFeesHourlyProtobuf, IndexerFeesProtobuf, IndexerQosHourlyProtobuf,
        IndexerQosProtobuf,
    },
    print_unix_millis,
};
use tokio::{sync::mpsc, task::JoinHandle};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

const AGGREGATION_INTERVAL_MINUTES: u32 = 60;
const AGGREGATION_INTERVAL_MILLIS: i64 = AGGREGATION_INTERVAL_MINUTES as i64 * 60000;

#[derive(Deserialize)]
struct Config {
    kafka: BTreeMap<String, String>,
    #[serde(default)]
    legacy_topics: bool,
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
    let producer_config = titorelli::kafka::config_with(
        config.kafka.clone(),
        [("compression.codec", "gzip")],
        &["group.id"],
    )?;
    let producer: rdkafka::producer::FutureProducer = producer_config.create()?;

    let mut legacy_producer: Option<ThreadedProducer<DefaultProducerContext>> = None;
    if config.legacy_topics {
        legacy_producer = Some(producer_config.create()?);
    }

    let start_timestamp =
        latest_sink_timestamp(&consumer).await?.unwrap_or(0) + AGGREGATION_INTERVAL_MILLIS;
    consumer.unassign().context("unassign sinks")?;
    tracing::info!(start_timestamp = print_unix_millis(start_timestamp)?);

    // Legacy topic messages are not aggregations so they risk duplicating large amounts of data
    // when we seek to start_timestamp. So we need to grab the committed offsets first to minimize
    // the duplication.
    let legacy_source_offsets: BTreeMap<i32, i64> = {
        let mut partitions = TopicPartitionList::new();
        for partition in fetch_partition_ids(consumer.client(), "gateway_queries")? {
            partitions.add_partition("gateway_queries", partition);
        }
        consumer
            .committed_offsets(partitions, Duration::from_secs(30))?
            .elements()
            .into_iter()
            .filter_map(|e| Some((e.partition(), e.offset().to_raw()?)))
            .collect()
    };

    let assignment = assign_partitions(&consumer, &["gateway_queries"], start_timestamp).await?;

    let (source_msg_tx, mut source_msg_rx) = mpsc::channel::<SourceMsg>(1024);
    let mut partition_consumers: Vec<JoinHandle<()>> = assignment
        .elements()
        .into_iter()
        .map(|e| {
            spawn_partition_consumer(
                &consumer,
                e.topic(),
                e.partition(),
                source_msg_tx.clone(),
                legacy_source_offsets.clone(),
            )
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
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => anyhow::bail!("exit"),
            _ = sigterm.recv() => anyhow::bail!("exit"),
            _ = consumer.recv() => anyhow::bail!("message received from split consumer"),
            _ = select_all(&mut partition_consumers) => anyhow::bail!("parition consumer exit"),
            _ = source_msg_rx.recv_many(&mut msg_buffer, msg_buffer_limit) => {
                anyhow::ensure!(!msg_buffer.is_empty(), "consumer channel closed");
                for msg in msg_buffer.drain(..) {
                    handle_source_msg(
                        &mut aggregations,
                        &mut flush_timestamps,
                        start_timestamp,
                        &producer,
                        msg,
                        &legacy_producer,
                    )
                    .await?;
                }
            }
        };
    }
}

async fn handle_source_msg(
    aggregations: &mut BTreeMap<i64, Aggregations>,
    flush_timestamps: &mut BTreeMap<String, i64>,
    start_timestamp: i64,
    producer: &FutureProducer,
    msg: SourceMsg,
    legacy_producer: &Option<ThreadedProducer<DefaultProducerContext>>,
) -> anyhow::Result<()> {
    match msg {
        SourceMsg::Flush {
            partition_id,
            aggregation_timestamp,
        } => {
            *flush_timestamps.get_mut(&partition_id).unwrap() = aggregation_timestamp;
            let min_timestamp = flush_timestamps.values().cloned().min().unwrap_or(0);
            while aggregations
                .first_key_value()
                .map(|(k, _)| *k <= min_timestamp)
                .unwrap_or(false)
            {
                let (t, agg) = aggregations.pop_first().unwrap();
                record_aggregations(producer, t, agg).await?;
                tracing::info!(timestamp = print_unix_millis(t).unwrap(), "flushed");
            }
        }
        SourceMsg::ClientQuery {
            timestamp,
            aggregation_timestamp,
            aggregation_only,
            data,
        } => {
            if aggregation_timestamp >= start_timestamp {
                let agg = aggregations.entry(aggregation_timestamp).or_default();

                if let Some(deployment) = data.indexer_queries.first().map(|i| &i.deployment) {
                    let key = ClientFeesKey {
                        gateway_id: data.gateway_id.clone(),
                        user_id: data.user_id.clone().unwrap_or_default(),
                        api_key: data.api_key.clone(),
                        deployment: deployment_cid(deployment),
                    };
                    let value = agg.client_fees.entry(key).or_default();
                    value.fees_grt += data.indexer_queries.iter().map(|i| i.fee_grt).sum::<f64>();
                    value.fees_usd += data.total_fees_usd;
                    value.total_response_time_ms += data.response_time_ms as f64;
                    match data.result.as_str() {
                        "success" => value.success_count += 1,
                        _ => value.failure_count += 1,
                    };
                }

                for indexer_query in &data.indexer_queries {
                    let key = IndexerFeesKey {
                        signer: Address::from_slice(&data.receipt_signer)?,
                        receiver: Address::from_slice(&indexer_query.indexer)?,
                    };
                    *agg.indexer_fees.entry(key).or_default() += indexer_query.fee_grt;
                }

                for indexer_query in &data.indexer_queries {
                    let key = IndexerQosKey {
                        indexer: Address::from_slice(&indexer_query.allocation)?,
                        deployment: deployment_cid(&indexer_query.deployment),
                    };
                    let value = agg
                        .indexer_qos
                        .entry(key)
                        .or_insert_with(|| IndexerQosValue {
                            chain: indexer_query.indexed_chain.clone(),
                            success_count: 0,
                            failure_count: 0,
                            total_seconds_behind: 0,
                            total_latency_ms: 0,
                            total_fee_grt: 0.0,
                        });
                    if indexer_query.result == "success" {
                        value.success_count += 1;
                    } else {
                        value.failure_count += 1;
                    }
                    value.total_seconds_behind += indexer_query.seconds_behind as u64;
                    value.total_latency_ms += indexer_query.response_time_ms as u64;
                    value.total_fee_grt += indexer_query.fee_grt;
                }
            }

            let legacy_producer = match legacy_producer {
                Some(legacy_producer) if !aggregation_only => legacy_producer,
                _ => return Ok(()),
            };
            let (client_query_result, indexer_attempts) = legacy_messages(timestamp, data);
            let record_value = serde_json::to_vec(&client_query_result).unwrap();
            let record = rdkafka::producer::BaseRecord::to("gateway_client_query_results")
                .key(&())
                .payload(&record_value);
            legacy_producer.send(record).map_err(|(err, _)| err)?;
            for indexer_attempt in indexer_attempts {
                let record_value = serde_json::to_vec(&indexer_attempt).unwrap();
                let record = rdkafka::producer::BaseRecord::to("gateway_indexer_attempts")
                    .key(&())
                    .payload(&record_value);
                legacy_producer.send(record).map_err(|(err, _)| err)?;
            }
        }
    }
    Ok(())
}

async fn latest_sink_timestamp(consumer: &StreamConsumer) -> anyhow::Result<Option<i64>> {
    let latest_messages = latest_messages(
        consumer,
        &["gateway_client_fees_hourly", "gateway_indexer_fees_hourly"],
    )
    .await?;
    let timestamp = latest_messages
        .into_iter()
        .map(|msg| -> anyhow::Result<i64> {
            let payload = msg.payload().context("missing_payload")?;
            match msg.topic() {
                "gateway_client_fees_hourly" => {
                    Ok(ClientFeesHourlyProtobuf::decode(payload)?.timestamp)
                }
                "gateway_indexer_fees_hourly" => {
                    Ok(IndexerFeesHourlyProtobuf::decode(payload)?.timestamp)
                }
                topic => anyhow::bail!("unhandled topic: {topic}"),
            }
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
    legacy_source_offsets: BTreeMap<i32, i64>,
) -> JoinHandle<()> {
    let id = format!("{topic}/{partition}");
    let partition_queue = consumer.split_partition_queue(topic, partition).unwrap();
    async fn handle_stream(
        id: String,
        mut stream: MessageStream<'_, DefaultConsumerContext>,
        source_msg_tx: mpsc::Sender<SourceMsg>,
        legacy_source_offsets: BTreeMap<i32, i64>,
    ) -> anyhow::Result<()> {
        let mut last_aggregation_timestamp = 0;
        loop {
            let msg = stream.next().await.context("stream closed")??;
            let msg = SourceMsg::decode(msg, &legacy_source_offsets)?;
            let aggregation_timestamp = match &msg {
                SourceMsg::Flush { .. } => unreachable!(), // unreachable
                SourceMsg::ClientQuery {
                    aggregation_timestamp,
                    ..
                } => *aggregation_timestamp,
            };
            let _ = source_msg_tx.send(msg).await;

            if aggregation_timestamp <= last_aggregation_timestamp {
                continue;
            }
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
    tokio::spawn(async move {
        let stream = partition_queue.stream();
        if let Err(err) =
            handle_stream(id.clone(), stream, source_msg_tx, legacy_source_offsets).await
        {
            tracing::error!("{:#}", err.context(id));
        }
    })
}

async fn record_aggregations(
    producer: &FutureProducer,
    timestamp: i64,
    aggregations: Aggregations,
) -> anyhow::Result<()> {
    let record_key = timestamp.to_be_bytes();
    let Aggregations {
        client_fees,
        indexer_fees,
        indexer_qos,
    } = aggregations;

    let record_payload = ClientFeesHourlyProtobuf {
        timestamp,
        aggregations: client_fees
            .into_iter()
            .map(|(k, v)| {
                let query_count = (v.success_count + v.failure_count).max(1) as f64;
                ClientFeesProtobuf {
                    gateway_id: k.gateway_id,
                    user_id: k.user_id,
                    api_key: k.api_key,
                    deployment: k.deployment,
                    fees_grt: v.fees_grt,
                    fees_usd: v.fees_usd,
                    query_count: v.success_count + v.failure_count,
                    success_rate: (v.success_count as f64 / query_count) as f32,
                    avg_response_time_ms: (v.total_response_time_ms / query_count) as u32,
                }
            })
            .collect(),
    }
    .encode_to_vec();
    let record = rdkafka::producer::FutureRecord::to("gateway_client_fees_hourly")
        .key(&record_key)
        .payload(&record_payload);
    producer
        .send(record, Duration::from_secs(30))
        .await
        .map_err(|(err, _)| err)
        .context("send aggregation record")?;

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
    let record = rdkafka::producer::FutureRecord::to("gateway_indexer_fees_hourly")
        .key(&record_key)
        .payload(&record_payload);
    producer
        .send(record, Duration::from_secs(30))
        .await
        .map_err(|(err, _)| err)
        .context("send aggregation record")?;

    let record_payload = IndexerQosHourlyProtobuf {
        timestamp,
        aggregations: indexer_qos
            .into_iter()
            .map(|(k, v)| {
                let total_queries = (v.success_count + v.failure_count) as f64;
                IndexerQosProtobuf {
                    indexer: k.indexer.0.to_vec(),
                    deployment: k.deployment,
                    chain: v.chain,
                    success_count: v.success_count,
                    failure_count: v.failure_count,
                    avg_seconds_behind: (v.total_seconds_behind as f64 / total_queries) as u64,
                    avg_latency_ms: (v.total_latency_ms as f64 / total_queries) as u32,
                    avg_fee_grt: (v.total_fee_grt / total_queries),
                }
            })
            .collect(),
    }
    .encode_to_vec();
    let record = rdkafka::producer::FutureRecord::to("gateway_indexer_qos_hourly")
        .key(&record_key)
        .payload(&record_payload);
    producer
        .send(record, Duration::from_secs(30))
        .await
        .map_err(|(err, _)| err)
        .context("send aggregation record")?;

    Ok(())
}

#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
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

fn deployment_cid(bytes: &[u8]) -> String {
    let mut buf = [0_u8; 34];
    buf[0..2].copy_from_slice(&[0x12, 0x20]);
    buf[2..].copy_from_slice(bytes);
    bs58::encode(buf).into_string()
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum SourceMsg {
    Flush {
        partition_id: String,
        aggregation_timestamp: i64,
    },
    ClientQuery {
        timestamp: i64,
        aggregation_timestamp: i64,
        aggregation_only: bool,
        data: ClientQueryProtobuf,
    },
}

impl SourceMsg {
    fn decode(
        msg: rdkafka::message::BorrowedMessage,
        legacy_source_offsets: &BTreeMap<i32, i64>,
    ) -> anyhow::Result<Self> {
        let timestamp = msg
            .timestamp()
            .to_millis()
            .ok_or_else(|| anyhow!("missing timestamp"))?;
        let aggregation_timestamp = timestamp - (timestamp % AGGREGATION_INTERVAL_MILLIS);
        let payload = msg.payload().context("missing payload")?;
        match msg.topic() {
            "gateway_queries" => {
                let decoded = ClientQueryProtobuf::decode(payload).context("decode protobuf")?;
                let aggregation_only =
                    msg.offset() < *legacy_source_offsets.get(&msg.partition()).unwrap_or(&0);
                Ok(SourceMsg::ClientQuery {
                    timestamp,
                    aggregation_timestamp,
                    aggregation_only,
                    data: decoded,
                })
            }
            topic => anyhow::bail!("unexpected topic: {topic}"),
        }
    }
}

#[derive(Debug, Default)]
struct Aggregations {
    client_fees: BTreeMap<ClientFeesKey, ClientFeesValue>,
    indexer_fees: BTreeMap<IndexerFeesKey, f64>,
    indexer_qos: BTreeMap<IndexerQosKey, IndexerQosValue>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ClientFeesKey {
    gateway_id: String,
    user_id: String,
    api_key: String,
    deployment: String,
}

#[derive(Debug, Default)]
struct ClientFeesValue {
    fees_grt: f64,
    fees_usd: f64,
    success_count: u32,
    failure_count: u32,
    total_response_time_ms: f64,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct IndexerFeesKey {
    signer: Address,
    receiver: Address,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct IndexerQosKey {
    indexer: Address,
    deployment: String,
}

#[derive(Debug)]
struct IndexerQosValue {
    chain: String,
    success_count: u32,
    failure_count: u32,
    total_seconds_behind: u64,
    total_latency_ms: u64,
    total_fee_grt: f64,
}

pub fn legacy_messages(
    timestamp: i64,
    client_query: ClientQueryProtobuf,
) -> (serde_json::Value, Vec<serde_json::Value>) {
    let address_hex = |bytes| format!("{:?}", Address::from_slice(bytes).unwrap());
    let first_indexer_query = client_query.indexer_queries.first();
    let user_id = client_query
        .user_id
        .unwrap_or("0x0000000000000000000000000000000000000000".into());
    let client_request_payload = serde_json::json!({
        "gateway_id": address_hex(&client_query.receipt_signer),
        "query_id": &client_query.query_id,
        "ray_id": &client_query.query_id,
        "network_chain": &client_query.gateway_id,
        "graph_env": &client_query.gateway_id,
        "timestamp": timestamp,
        "api_key": &client_query.api_key,
        "user": &user_id,
        "deployment": first_indexer_query.map(|i| deployment_cid(&i.deployment)).unwrap_or_default(),
        "subgraph": &client_query.subgraph,
        "indexed_chain": first_indexer_query.map(|i| i.indexed_chain.clone()).unwrap_or_default(),
        "network": first_indexer_query.map(|i| i.indexed_chain.clone()).unwrap_or_default(),
        "response_time_ms": client_query.response_time_ms,
        "request_bytes": client_query.request_bytes,
        "response_bytes": client_query.response_bytes,
        "budget": 40e-6_f32.to_string(),
        "query_count": 1,
        "fee": client_query.indexer_queries.iter().map(|i| i.fee_grt).sum::<f64>() as f32,
        "fee_usd": client_query.total_fees_usd as f32,
        "status": match &client_query.result {
            r if r == "success" => "200 OK",
            r if r.starts_with("bad indexers:") => "No suitable indexer found for subgraph deployment",
            r if r.starts_with("block not found:") => "Unresolved block",
            r if r.starts_with("internal error:") => "Internal error",
            r if r.starts_with("auth error:") => "Invalid API key",
            r if r.starts_with("bad query:") => "Invalid query",
            r if r.starts_with("no indexers found") => "No indexers found for subgraph deployment",
            r => r,
        },
        "status_code": match &client_query.result {
            r if r == "success" => 0_u32,
            r if r.starts_with("bad indexers:") => 510359393,
            r if r.starts_with("block not found:") => 604610595,
            r if r.starts_with("internal error:") => 816601499,
            r if r.starts_with("auth error:") => 888904173,
            r if r.starts_with("bad query:") => 595700117,
            r if r.starts_with("no indexers found") => 1621366907,
            r if r.starts_with("subgraph not found:") => 2599148187,
            _ => 0,
        },
    });
    let indexer_attempt_payloads = client_query
        .indexer_queries
        .iter()
        .map(|i| {
            serde_json::json!({
                "gateway_id": address_hex(&client_query.receipt_signer),
                "query_id": &client_query.query_id,
                "ray_id": &client_query.query_id,
                "network_chain": &client_query.gateway_id,
                "graph_env": &client_query.gateway_id,
                "timestamp": timestamp,
                "api_key": &client_query.api_key,
                "user_address": &user_id,
                "deployment": deployment_cid(&i.deployment),
                "network": &i.indexed_chain,
                "indexed_chain": &i.indexed_chain,
                "indexer": address_hex(&i.indexer),
                "url": &i.url,
                "fee": i.fee_grt as f32,
                "legacy_scalar": false,
                "utility": 1.0,
                "seconds_behind": i.seconds_behind,
                "blocks_behind": i.blocks_behind,
                "response_time_ms": i.response_time_ms,
                "allocation": address_hex(&i.allocation),
                "indexer_errors": &i.indexer_errors,
                "status": match &i.result {
                    r if r.contains("success") => "200 OK",
                    r => r,
                },
                "status_code": match &i.result {
                    r if r.contains("success") => 200_u32.to_be() & (u32::MAX >> 4),
                    r if r.contains("Internal") => 0x1_u32 << 28,
                    r if r.contains("Unavailable") => 0x2_u32 << 28,
                    r if r.contains("Timeout") => 0x3_u32 << 28,
                    r if r.contains("BadResponse") => 0x4_u32 << 28,
                    _ => 0,
                },
            })
        })
        .collect();
    (client_request_payload, indexer_attempt_payloads)
}
