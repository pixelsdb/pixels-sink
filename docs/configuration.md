# Configuration Reference

Pixels Sink is configured via a Java properties file. Pass the path with `-c`.

- Examples: `conf/pixels-sink.pg.properties`, `conf/pixels-sink.aws.properties`, `conf/pixels-sink.flink.properties`

Values are loaded by `PixelsSinkConfig` and mapped from keys in the properties file.

## Core Keys

| Key | Default | Notes |
| --- | --- | --- |
| `sink.datasource` | `engine` | Source type: `engine`, `kafka`, or `storage`. |
| `sink.mode` | `retina` | Sink type: `retina`, `csv`, `proto`, `flink`, or `none`. |
| `sink.datasource.decode.threads` | `4` | Decode thread pool size. Engine uses `StreamOrderedDecoder` (multi logical stream); Storage uses `OrderedBatchDecoder` (single-key batch). |
| `sink.datasource.engine.format` | `connect` | Engine wire format. Only `connect` is allowed; other values fail fast on resolve. |
| `sink.datasource.rate.limit` | `-1` | Rate limit for source ingestion. `-1` disables. |
| `sink.datasource.rate.limit.type` | `semaphore` | Rate limiter type used by `FlushRateLimiterFactory`. 'guava' or 'semaphore'|

### Notes on `sink.datasource`

- `engine` reads CDC logs directly from Debezium Engine. Currently requires `sink.datasource.engine.format=connect`. Json/Avro Engine paths are reserved and will reuse the same `conversion.debezium` converter interfaces later.
- `storage` reads CDC logs from files dumped by `sink.proto` output; schema reference: [sink.proto](https://github.com/pixelsdb/pixels/blob/master/proto/sink.proto). 
- `kafka` reads from a set of Kafka topics; this mode is deprecated and not actively tested.
- Engine and Kafka records are normalized to the canonical `SinkProto` contract before reaching writers.
- Storage row records use canonical `SinkProto`; `SourceInfo.schema` may be empty for MySQL.

### Notes on `sink.mode`

- `retina` connects to one or more Retina services via RPC and sends `UpdateRecord` or `StreamUpdateRecord` requests defined in [retina.proto](https://github.com/pixelsdb/pixels/blob/master/proto/retina.proto). 
- `csv` is mainly for debugging. 
- `proto` converts row change events and transaction metadata into `sink.proto` format, writes them in order to one or more files, and registers file paths in ETCD. These files can be read by `sink.datasource=storage`. This provides the highest CDC read efficiency and is used in paper experiments. 
- `flink` starts a server for external programs to pull data via RPC and continue ingestion, for example [pixels-lance](https://github.com/AntiO2/pixels-lance) or [pixels-flink](https://github.com/AntiO2/pixels-flink). 
- `none` writes no output and is useful for testing or observing source-side metrics.

## Source and Sink

### Transaction

Only supported in **Retina** sink mode.

| Key | Default | Notes |
| --- | --- | --- |
| `sink.trans.batch.size` | `100` | Batch size for transaction processing. |
| `sink.trans.mode` | `batch` | Transaction mode: `single`, `record`, or `batch`. |
| `transaction.timeout` | `300` | Transaction timeout in seconds. |

Notes on `sink.trans.mode`: 
- `single` means each Retina request writes exactly one transaction. 
- `batch` means a single Retina request may carry multiple transactions. 
- `single` and `batch` both support cross-table transactions. `record` disables cross-table transactions and only processes single-table transactions.

### Debezium Engine Source

| Key | Default | Notes |
| --- | --- | --- |
| `sink.datasource.engine.format` | `connect` | Only `connect` is runnable today. Non-`connect` values fail fast. |
| `sink.debezium.dialect` | none | Sink-side CDC dialect for envelope normalization: `mysql` or `postgresql`. Used by Engine and Kafka conversion. If unset, falls back to inferring from `debezium.connector.class`. |
| `debezium.name` | none | Engine name. |
| `debezium.connector.class` | none | Debezium Engine connector class only (which DB the engine connects to). Not a Kafka setting. |
| `debezium.*` | none | Standard Debezium engine properties. |

See `conf/pixels-sink.mysql.properties` for a TDSQL MySQL CDC example.

The Debezium Connector reads the database Binlog or WAL. The local `conversion.debezium` package only converts envelopes already emitted by Debezium (`connect` / `json` / `avro` plus shared `support` / `dialect`).

### Retina Sink

| Key | Default | Notes |
| --- | --- | --- |
| `sink.retina.mode` | `stub` | Write mode: `stub` or `stream`. |
| `sink.retina.client` | `1` | Number of Retina clients per table writer. |
| `sink.retina.log.queue` | `true` | Enable queue logging. |
| `sink.retina.rpc.limit` | `1000` | Max inflight RPC requests. |
| `sink.retina.trans.limit` | `1000` | Max inflight transaction requests. |
| `sink.retina.trans.request.batch` | `false` | Enable batched transaction requests. |
| `sink.retina.trans.request.batch.size` | `100` | Batch size for transaction requests. |
| `sink.timeout.ms` | `30000` | RPC timeout. |
| `sink.flush.interval.ms` | `1000` | Flush interval. |
| `sink.flush.batch.size` | `100` | Flush batch size. |
| `sink.max.retries` | `3` | Retry limit. |
| `sink.commit.method` | `async` | Commit method: `sync` or `async`. |
| `sink.commit.batch.size` | `500` | Commit batch size. |
| `sink.commit.batch.worker` | `16` | Commit worker threads. |
| `sink.commit.batch.delay` | `200` | Commit batch delay in ms. |


### CSV Sink

| Key | Default | Notes |
| --- | --- | --- |
| `sink.csv.path` | `./data` | Output directory. |
| `sink.csv.enable_header` | `false` | Write header row. |

### Proto Sink and Storage Source

| Key | Default | Notes |
| --- | --- | --- |
| `sink.proto.dir` | required | Proto output or input directory. |
| `sink.proto.data` | `data` | Data set name. |
| `sink.proto.maxRecords` | `100000` | Max records per file. |
| `sink.storage.mode` | `stream` | Storage read mode: `stream` reads records incrementally; `memory` preloads all records before replay. |
| `sink.storage.loop` | `false` | Whether to loop over stored files. |

### Flink Sink

| Key | Default | Notes |
| --- | --- | --- |
| `sink.flink.server.port` | `9091` | Polling server port. |


### Kafka Source

Kafka source is deprecated.

| Key | Default | Notes |
| --- | --- | --- |
| `bootstrap.servers` | required | Kafka bootstrap servers. |
| `group.id` | required | Consumer group id. |
| `auto.offset.reset` | none | Standard Kafka consumer property. |
| `key.deserializer` | `org.apache.kafka.common.serialization.StringDeserializer` | Kafka key deserializer. |
| `sink.kafka.value.format` | `json` | Envelope format: `json` or `avro`. Kafka sources assemble `conversion.debezium` converters from this key. |
| `sink.debezium.dialect` | none | Upstream CDC dialect (`mysql` / `postgresql`). Required for Kafka transaction decoding; row events can also infer from `source.connector` when unset. |
| `topic.prefix` | required | Topic prefix for table events. |
| `consumer.capture_database` | required | Database name used to build topic names. |
| `consumer.include_tables` | empty | Comma-separated table list, empty means all. |
| `transaction.topic.suffix` | `transaction` | Suffix appended to transaction topics. |
| `transaction.topic.group_id` | `transaction_consumer` | Consumer group for transaction topic. |
| `sink.registry.url` | required | Avro Schema registry endpoint. |

**Legacy Kafka deserializer migration**

Prefer `sink.kafka.value.format`. Old keys `value.deserializer` and
`transaction.topic.value.deserializer` are no longer SPI; `PixelsSinkConfig`
migrates them at startup:

| Condition | Behavior |
| --- | --- |
| `sink.kafka.value.format` is set; legacy keys still present | Warn and ignore legacy keys |
| Format unset; legacy class name maps to Json/Avro | Infer format and warn |
| Row/tx inferences conflict, or class name unrecognized | Fail fast |

**Reserved Configuration**

| Key | Default | Notes |
| --- | --- | --- |
| `sink.remote.host` | `localhost` | Sink server host. |
| `sink.remote.port` | `9090` | Sink server port. |
| `sink.rpc.enable` | `false` | Enable RPC simulation (for development). |

**Monitoring and Metrics**

| Key | Default | Notes |
| --- | --- | --- |
| `sink.monitor.enable` | `false` | Enable Prometheus metrics endpoint. |
| `sink.monitor.port` | `9464` | Metrics server port. |
| `sink.monitor.report.enable` | `true` | Enable report file output. |
| `sink.monitor.report.interval` | `5000` | Report interval in ms. |
| `sink.monitor.report.file` | `/tmp/sink.csv` | Report output file. |
| `sink.monitor.freshness.interval` | `1000` | Freshness report interval in ms. |
| `sink.monitor.freshness.file` | `/tmp/sinkFreshness.csv` | Freshness report output file. |
| `sink.monitor.freshness.level` | `row` | `row`, `txn`, or `embed`. |
| `sink.monitor.freshness.embed.warmup` | `10` | Warmup seconds for embedded freshness query. |
| `sink.monitor.freshness.embed.static` | `false` | Whether to keep a static snapshot. |
| `sink.monitor.freshness.embed.snapshot` | `false` | Whether to take a snapshot. |
| `sink.monitor.freshness.embed.tablelist` | empty | Tables to include for embedded mode. |
| `sink.monitor.freshness.embed.delay` | `0` | Delay seconds for embedded freshness query. |
| `sink.monitor.freshness.verbose` | `false` | Verbose freshness logging. |
| `sink.monitor.freshness.timestamp` | `false` | Include timestamps. |

Note: In the Retina paper experiments, `sink.monitor.freshness.level=embed` queries freshness through JDBC. This requires the last column of each table to be `freshness_ts`. Trino is available in the default build; HiveServer2 support for Hudi requires building with `-Phudi-hive`.

**Freshness Query Settings**

| Key | Default | Notes |
| --- | --- | --- |
| `sink.query.url` | required for embedded freshness | Trino or HiveServer2 JDBC URL. |
| `sink.query.user` | required for embedded freshness | Username. |
| `sink.query.password` | empty | Password. |
| `sink.query.parallel` | `1` | Parallel query count. |
