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
      string api_key = 1;
      string deployment = 2;
      double fees_grt = 3;
      double fees_usd = 4;
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
