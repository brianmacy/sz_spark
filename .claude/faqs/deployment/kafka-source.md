# Kafka source for the parallel-batch feeder (`glue.KafkaSource`, Step 2)

## What it is

The watermark-flavor [`RecordSource`] for `glue.ParquetParallelFeeder` — the object-store-safe,
Databricks-native counterpart to the on-prem `InboxSource`. Same `OverlappingBatchEngine`, same
`AddCore`; only the source differs. Select it with `source=kafka`.

## Locked design decisions (do NOT re-litigate)

- **ONE unpartitioned topic (partition 0).** Read parallelism comes from **`minPartitions`** fanning
  that single Kafka partition into N Spark read tasks — NOT from Kafka partitions. Rationale: Kafka
  partitioning would group records by the partition key; if that correlates with a resolution key it
  creates cross-key entity-lock contention — the exact thing the project avoids. Keep the topic
  single-partition.
- **Count-bounded batches, never "read to latest."** `nextChunk` claims
  `[cursor, min(cursor + recordsPerBatch, latest))`. Bounding by a record COUNT is what preserves the
  1-partition-per-batch operating point: a large consumer lag becomes many small batches, not one
  giant straggler-prone batch. `latest` is the driver-side `endOffsets` metadata for partition 0.
- **Contiguous-completed-prefix watermark.** The engine's K workers `commit` out of order, but a
  monotonic offset may only advance over the contiguous-completed prefix. `glue.OffsetWatermark` holds
  a completed-but-behind-a-gap range until the gap fills, then sweeps forward and persists. The
  checkpoint is double-buffered (`checkpoint/offset-<topic>-0` + a `.bak` sibling) so a crash mid-write
  still leaves a one-behind offset to recover from.
- **`reclaim` is a no-op.** A restart re-reads from the committed offset; the replayed tail
  (completed-but-behind-a-straggler ranges that were never persisted) is a handful of cheap optimized
  no-op re-adds — at-least-once, never-drop, same contract as the dispose flavor.

## Launching

```
spark-submit --class com.senzing.spark.glue.ParquetParallelFeeder \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:<sparkVersion> \
  sz-spark-assembly.jar \
  source=kafka bootstrapServers=broker:9092 topic=sz-records \
  checkpoint=<durable dir> startingOffset=earliest minPartitions=1 \
  recordsPerBatch=1000 maxUnprocessedBatches=<>= spark.cores.max> \
  deadLetter=<dir> output=<dir> runId=<id>
```

- The `spark-sql-kafka-0-10` connector is a **`Provided`** dependency — it is NOT bundled in the FAT
  jar (would bloat/conflict). Supply it at launch with `--packages` (or it is already present on
  Databricks clusters). It brings `kafka-clients` transitively for the driver-side `endOffsets` call.
- `startingOffset` (`earliest` | `latest` | `<number>`) is honored **only on a cold start** (no
  checkpoint yet). After the first commit the durable checkpoint governs where reading resumes; a
  different `startingOffset` on a later run is ignored.
- The Kafka `value` is the raw JSON record body (identical to the RabbitMQ path). `DATA_SOURCE` /
  `RECORD_ID` are projected on the executors with `get_json_object` (no driver-side Jackson
  bottleneck); the whole body is the payload. A record missing `DATA_SOURCE`/`RECORD_ID` follows the
  same dead-letter path as every other source.

## Testing

- `OffsetWatermarkSpec` — the out-of-order / gap / idempotency / durability / restart invariants on a
  real local `LocalFileSystem` (no mocks). This is the correctness-critical logic.
- `KafkaSourceSpec` — the count-bounding (`nextRange`) and `bounds` round-trip (`parseBounds`), pure
  and broker-free.
- End-to-end against a live broker is an `IntegrationTest` (tagged, excluded from `sbt test`), like
  `EngineIT` — it needs a real Kafka.

## Not yet built (rest of Step 2)

- **RabbitMQ→Kafka bridge:** read RabbitMQ → produce to the topic, throttled so the Spark consumer
  stays ≤ ~5M records behind (`latestOffset − committedOffset` cap). Kafka is the durable buffer.
- **`DeltaSource`:** Delta table version / CDF watermark — the same seam, cleanest on Databricks.
