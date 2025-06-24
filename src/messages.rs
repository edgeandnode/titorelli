#[derive(prost::Message)]
pub struct ClientQueryProtobuf {
    #[prost(string, tag = "1")]
    pub gateway_id: String,
    // 20 bytes (address)
    #[prost(bytes, tag = "2")]
    pub receipt_signer: Vec<u8>,
    #[prost(string, tag = "3")]
    pub query_id: String,
    #[prost(string, tag = "4")]
    pub api_key: String,
    #[prost(string, optional, tag = "11")]
    pub user_id: Option<String>,
    #[prost(string, optional, tag = "12")]
    pub subgraph: Option<String>,
    #[prost(string, tag = "5")]
    pub result: String,
    #[prost(uint32, tag = "6")]
    pub response_time_ms: u32,
    #[prost(uint32, tag = "7")]
    pub request_bytes: u32,
    #[prost(uint32, optional, tag = "8")]
    pub response_bytes: Option<u32>,
    #[prost(double, tag = "9")]
    pub total_fees_usd: f64,
    #[prost(message, repeated, tag = "10")]
    pub indexer_queries: Vec<IndexerQueryProtobuf>,
}

#[derive(prost::Message)]
pub struct IndexerQueryProtobuf {
    /// 20 bytes (address)
    #[prost(bytes, tag = "1")]
    pub indexer: Vec<u8>,
    /// 32 bytes
    #[prost(bytes, tag = "2")]
    pub deployment: Vec<u8>,
    /// 20 bytes (address)
    #[prost(bytes, tag = "3")]
    pub allocation: Vec<u8>,
    #[prost(string, tag = "4")]
    pub indexed_chain: String,
    #[prost(string, tag = "5")]
    pub url: String,
    #[prost(double, tag = "6")]
    pub fee_grt: f64,
    #[prost(uint32, tag = "7")]
    pub response_time_ms: u32,
    #[prost(uint32, tag = "8")]
    pub seconds_behind: u32,
    #[prost(string, tag = "9")]
    pub result: String,
    #[prost(string, tag = "10")]
    pub indexer_errors: String,
    #[prost(uint64, tag = "11")]
    pub blocks_behind: u64,
}

#[derive(prost::Message)]
pub struct ClientFeesHourlyProtobuf {
    /// start timestamp for aggregation, in unix milliseconds
    #[prost(int64, tag = "1")]
    pub timestamp: i64,
    #[prost(message, repeated, tag = "2")]
    pub aggregations: Vec<ClientFeesProtobuf>,
}

#[derive(prost::Message)]
pub struct ClientFeesProtobuf {
    #[prost(string, tag = "1")]
    pub gateway_id: String,
    #[prost(string, tag = "2")]
    pub user_id: String,
    #[prost(string, tag = "3")]
    pub api_key: String,
    #[prost(string, tag = "4")]
    pub deployment: String,
    #[prost(double, tag = "5")]
    pub fees_grt: f64,
    #[prost(double, tag = "6")]
    pub fees_usd: f64,
    #[prost(uint32, tag = "7")]
    pub query_count: u32,
    #[prost(float, tag = "8")]
    pub success_rate: f32,
    #[prost(uint32, tag = "9")]
    pub avg_response_time_ms: u32,
}

#[derive(prost::Message)]
pub struct IndexerFeesHourlyProtobuf {
    /// start timestamp for aggregation, in unix milliseconds
    #[prost(int64, tag = "1")]
    pub timestamp: i64,
    #[prost(message, repeated, tag = "2")]
    pub aggregations: Vec<IndexerFeesProtobuf>,
}

#[derive(prost::Message)]
pub struct IndexerFeesProtobuf {
    /// 20 bytes (address)
    #[prost(bytes, tag = "1")]
    pub signer: Vec<u8>,
    /// 20 bytes (address)
    #[prost(bytes, tag = "2")]
    pub receiver: Vec<u8>,
    #[prost(double, tag = "3")]
    pub fees_grt: f64,
}

#[derive(prost::Message)]
pub struct IndexerQosHourlyProtobuf {
    /// start timestamp for aggregation, in unix milliseconds
    #[prost(int64, tag = "1")]
    pub timestamp: i64,
    #[prost(message, repeated, tag = "2")]
    pub aggregations: Vec<IndexerQosProtobuf>,
}

#[derive(prost::Message)]
pub struct IndexerQosProtobuf {
    /// 20 bytes (address)
    #[prost(bytes, tag = "1")]
    pub indexer: Vec<u8>,
    #[prost(string, tag = "2")]
    pub deployment: String,
    #[prost(string, tag = "3")]
    pub chain: String,
    #[prost(uint32, tag = "4")]
    pub success_count: u32,
    #[prost(uint32, tag = "5")]
    pub failure_count: u32,
    #[prost(uint64, tag = "6")]
    pub avg_seconds_behind: u64,
    #[prost(uint32, tag = "7")]
    pub avg_latency_ms: u32,
    #[prost(double, tag = "8")]
    pub avg_fee_grt: f64,
}
