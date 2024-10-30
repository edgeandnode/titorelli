pub mod kafka;
pub mod messages;

use anyhow::Context as _;
use chrono::DateTime;

pub fn print_unix_millis(millis: i64) -> anyhow::Result<String> {
    let t = DateTime::from_timestamp_millis(millis).context("timestamp out of range")?;
    Ok(t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
}
