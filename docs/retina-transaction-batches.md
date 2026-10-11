# Retina transaction batches

The Retina payload builder folds repeated primary keys within one source transaction and one RPC batch. It keeps the earliest before image and final after image:

| Changes | Net write |
| --- | --- |
| U → U | U with final values |
| I → U | I with final values |
| I → D | None |
| U → D | D |
| D → I | U with final values |
| I → U → D | None |
| U → D → I | U with final values |

Different transactions and RPC batches remain separate. Primary-key changes must be split into an old-key delete and new-key insert by the bucket dispatcher before batching. Snapshots and events without a source transaction ID are not folded. Each operation group has unique keys, so repeated keys without a safe transaction boundary execute in separate, ordered groups.

Transaction counters use the original event count after a successful RPC acknowledgment. An entirely canceled batch sends an empty RPC to retain the acknowledgment boundary; failed RPCs do not advance counters.

Coalescing uses only batch-local state and introduces no locks. Existing per-writer flush serialization and Retina stream processing retain ordering between RPCs; different buckets can still execute concurrently. Native visibility concurrency is handled separately in [Pixels PR #1412](https://github.com/pixelsdb/pixels/pull/1412).

Run focused regressions with:

```sh
mvn -Dtest=RetinaPayloadBuilderTest,TableCrossTxCoalescingTest test
```
