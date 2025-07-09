# titorelli

a service that reinterprets kafka messages from the gateway

## details

Currently, this service consumes from the `gateway_querits` topic, and produces hourly
aggregations into the following topics:

- `gateway_client_fees_hourly`

  ```protobuf
  syntax = "proto3";
  message ClientFeesHourly {
    message ClientFees {
      string gateway_id = 1;
      string user_id = 2;
      string api_key = 3;
      string deployment = 4;
      double fees_grt = 5;
      double fees_usd = 6;
      uint32 query_count = 7;
      float success_rate = 8;
      uint32 avg_response_time_ms = 9;
    }
    // start timestamp for aggregation, in unix milliseconds
    int64 timestamp = 1;
    repeated ClientFees aggregations = 2;
  }
  ```

- `gateway_indexer_fees_hourly`

  ```protobuf
  syntax = "proto3";
  message IndexerFeesHourly {
    message IndexerFees {
      // 20 bytes (address)
      bytes signer = 1;
      // 20 bytes (address)
      bytes receiver = 2;
      double fees_grt = 3;
    }
    // start timestamp for aggregation, in unix milliseconds
    int64 timestamp = 1;
    repeated IndexerFees aggregations = 2;
  }
  ```

- `gateway_indexer_qos_hourly`

  ```protobuf
  syntax = "proto3";
  message IndexerQosHourly {
    message IndexerQos {
      /// 20 bytes (address)
      bytes indexer = 1;
      string deployment = 2;
      string chain = 3;
      uint32 success_count = 4;
      uint32 failure_count = 5;
      uint64 avg_seconds_behind = 6;
      uint32 avg_latency_ms = 7;
      double avg_fee_grt = 8;
    }
    /// start timestamp for aggregation; in unix milliseconds
    int64 timestamp = 1;
    repeated IndexerQos aggregations = 2;
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
