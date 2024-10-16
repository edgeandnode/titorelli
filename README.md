# titorelli

a service that reinterprets kafka messages from the gateway

## details

Currently, this service consumes from the `gateway_indexer_fees` topic, and produces hourly
aggregations into the `gateway_indexer_fees_hourly` topic. Heres a schema for the relevant messages:

```protobuf
message IndexerFees {
  // 20 bytes (address)
  bytes signer = 1;
  // 20 bytes (address)
  bytes receiver = 2;
  double fees_grt = 3;
}

message IndexerFeesHourly {
  /// start timestamp for aggregation, in unix milliseconds
  int64 timestamp = 1;
  repeated IndexerFees aggregations = 2;
}
```

## example configuration

```json
{
  "kafka": {
    "bootstrap.servers": "example.com:9092",
    "group.id": "titorelli",
    "security.protocol": "sasl_ssl",
    "sasl.mechanism": "SCRAM-SHA-256",
    "sasl.username": "example_username",
    "sasl.password": "example_password",
    "ssl.ca.location": "./example.crt"
  }
}
```
