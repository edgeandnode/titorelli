use anyhow::{anyhow, Context as _};
use rdkafka::{
    consumer::{Consumer as _, StreamConsumer},
    message::OwnedMessage,
    Offset, TopicPartitionList,
};
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(30);

pub fn config_with<'a>(
    settings: impl IntoIterator<Item = (String, String)>,
    overrides: impl IntoIterator<Item = (&'a str, &'a str)>,
    without: &[&str],
) -> anyhow::Result<rdkafka::ClientConfig> {
    let mut config = rdkafka::ClientConfig::from_iter(
        settings
            .into_iter()
            .filter(|(k, _)| !without.contains(&k.as_str())),
    );
    for (key, value) in overrides {
        if matches!(config.get(key), Some(v) if v != value) {
            anyhow::bail!("kafka config entry {key} must be set to {value}");
        }
        config.set(key, value);
    }
    Ok(config)
}

pub fn fetch_partition_ids<C>(
    client: &rdkafka::client::Client<C>,
    topic: &str,
) -> anyhow::Result<Vec<i32>>
where
    C: rdkafka::client::ClientContext,
{
    let metadata = client
        .fetch_metadata(Some(topic), TIMEOUT)
        .with_context(|| anyhow!("fetch {topic} metadata"))?;
    anyhow::ensure!(!metadata.topics().is_empty());
    let topic_info = &metadata.topics()[0];
    Ok(topic_info.partitions().iter().map(|p| p.id()).collect())
}

pub async fn earliest_messages(
    consumer: &StreamConsumer,
    topics: &[&str],
) -> anyhow::Result<Vec<OwnedMessage>> {
    fetch_messages(consumer, topics, Offset::Beginning).await
}

pub async fn latest_messages(
    consumer: &StreamConsumer,
    topics: &[&str],
) -> anyhow::Result<Vec<OwnedMessage>> {
    fetch_messages(consumer, topics, Offset::OffsetTail(1)).await
}

pub async fn assign_partitions(
    consumer: &StreamConsumer,
    topics: &[&str],
    timestamp: i64,
) -> anyhow::Result<TopicPartitionList> {
    let mut timestamps = TopicPartitionList::new();
    for topic in topics {
        let partitions = fetch_partition_ids(consumer.client(), topic)?;
        for partition in partitions {
            timestamps.add_partition_offset(topic, partition, Offset::Offset(timestamp))?;
        }
    }
    tracing::debug!(?timestamps);

    let assignment = consumer.offsets_for_times(timestamps, TIMEOUT)?;
    tracing::debug!(?assignment);
    consumer.assign(&assignment)?;

    Ok(assignment)
}

async fn fetch_messages(
    consumer: &StreamConsumer,
    topics: &[&str],
    offset: Offset,
) -> anyhow::Result<Vec<OwnedMessage>> {
    let mut result: Vec<OwnedMessage> = Default::default();
    for topic in topics {
        let mut assignment = TopicPartitionList::new();
        for partition in fetch_partition_ids(consumer.client(), topic)? {
            // skip empty partitions
            let watermarks = consumer.fetch_watermarks(topic, partition, TIMEOUT)?;
            if (watermarks.1 - watermarks.0) > 0 {
                assignment.add_partition(topic, partition);
            }
        }
        if assignment.elements().is_empty() {
            continue;
        }
        assignment.set_all_offsets(offset)?;
        consumer
            .assign(&assignment)
            .with_context(|| anyhow!("assign {topic} partitions"))?;
        for _ in 0..assignment.elements().len() {
            let msg = consumer.recv().await.context("fetch latest message")?;
            result.push(msg.detach());
        }
    }
    Ok(result)
}
