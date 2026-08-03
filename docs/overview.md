# Pixels Sink Overview

![](./assets/frame.png)

Pixels Sink uses a multi-stage pipeline. Each stage communicates via producer/consumer queues.

**Entry**

**PixelsSinkApp**
- Main entry point for running as a standalone server.
- Configuration is loaded via `PixelsSinkConfigFactory` using the properties file passed by `-c`.

**PixelsSinkProvider**
- Implements Pixels SPI so it can be started by Pixels Worker.
- Receives a `ConfigFactory` directly and builds the same sink pipeline.

**Source**
The source stage owns transport lifecycle (Engine / Kafka / Storage). Payload
conversion is separate from transport:

- Parallel decode infrastructure lives in `util.concurrent`
  (`DecodeExecutors`, `StreamOrderedDecoder`, `OrderedBatchDecoder`) and is
  sized by `sink.datasource.decode.threads`.
- Debezium envelopes (Engine Connect / Kafka JSON / Kafka Avro) convert through
  `conversion.debezium` (`connect` / `json` / `avro` + `support` / `dialect`).
- Storage sink-proto bytes convert through `conversion.sinkproto` and stay
  independent of the Debezium package.
- Engine event classification stays in `source.engine` (`ConnectEventClassifier`,
  `DebeziumRecordType`).

**Decode ordering (not enhanced beyond historical contract)**

| Guaranteed | Not guaranteed |
| --- | --- |
| Same table (Engine) / same Storage key: FIFO relative to source scan or enqueue order | Cross-table global order |
| TX events FIFO on the TX stream | TX BEGIN/END vs row events global arrival order |
| | Same-table cross-PK-bucket write total order; `totalOrder` / `dataCollectionOrder` reorder |

Two decoders have different roles (not a shared abstraction):

| Component | Role | Stream / shard key |
| --- | --- | --- |
| `StreamOrderedDecoder` | Engine: ordered publish across multiple logical streams in a mixed batch | ROW = `SchemaTableName`; TX = singleton stream key |
| `OrderedBatchDecoder` | Storage: parallel decode inside one key consumer, then deliver in input order | Sharding already done by `queueMap`; decoder does not re-shard |

**Source Inputs**
| Source Type | Description | Related Config |
| --- | --- | --- |
| `engine` | Debezium Engine reads WAL/binlog directly from a database | `debezium.*`, `sink.datasource.engine.format` |
| `kafka` | Kafka consumer reads change events from topics | `bootstrap.servers`, `group.id`, `topic.*`, `sink.kafka.value.format` |
| `storage` | Reads from Pixels storage files containing serialized sink proto records | `sink.proto.*`, `sink.storage.loop` |

**Source Outputs**
- Kafka, Engine, and Storage sources publish only canonical
  `RowChangeEvent` or `SinkProto.TransactionMetadata` objects.
- Protocol conversion is implemented in `conversion`; pipeline queues never receive
  Kafka bytes, Engine `SourceRecord`, or Storage `ByteBuffer`.

**Pipeline Queue**
Pipelines own bounded canonical-event queues. The queue provides backpressure,
ordered delivery, and lifecycle management without knowing the source protocol.

```mermaid
classDiagram
    direction TB

    class BlockingBoundedQueue~T~ {
        +put(T value)
        +take()
        +close()
    }

    class TablePipeline {
        +publish(RowChangeEvent)
    }
    class TransactionPipeline {
        +publish(TransactionMetadata)
    }
    class TablePipelineManager {
        +route(RowChangeEvent)
    }

    TablePipelineManager --> TablePipeline
    TablePipeline --> BlockingBoundedQueue
    TransactionPipeline --> BlockingBoundedQueue

```

**Processor**
Processors pull events from pipeline queues and write to the sink writers.

- `TableProcessor` instances are created by `TablePipelineManager`.
- There is typically one `TableProcessor` per table to maintain per-table ordering.
- `TransactionPipeline` owns the transaction queue and `TransactionProcessor`.

**Writer**
Writers implement `PixelsSinkWriter`:

| Method | Description |
| --- | --- |
| `writeRow(RowChangeEvent rowChangeEvent)` | Write a row change |
| `writeTrans(SinkProto.TransactionMetadata transactionMetadata)` | Handle transaction metadata |
| `flush()` | Flush buffered data |

**Retina Writer**
`RetinaWriter` implements transactional replay into Retina.

Key components:
- `RetinaServiceProxy` communicates with Retina.
- `SinkContextManager` holds transaction context and table writer proxies.

Bucket routing for `RowChangeEvent`:
- Insert: derive bucket from the after-image key.
- Delete: derive bucket from the before-image key.
- Update: if primary key is unchanged, use any key. If primary key changes, split into delete and insert events and preserve delete-then-insert order.

![img.png](./assets/TransactionCoordinator.png)

Table writers:
- `SingleTxWriter` writes a single transaction per call.
- `CrossTxWriter` allows a batch to contain multiple transactions.

Transactions are committed via `TransactionProxy` which supports synchronous or async batch commits.

**Proto Writer**
Creates storage source files by serializing events to proto. Metadata (file paths, etc.) is stored in ETCD.

**CSV Writer**
Writes events to CSV files.

**Flink Writer**
Exposes events to Flink through a polling service.
