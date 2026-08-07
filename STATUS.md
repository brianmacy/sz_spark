# Project Status

**Date:** 2026-08-07
**Branch:** bem_kafka_source (not yet pushed / no PR)
**State:** the M0–M16 reference implementation, the streaming ingest path (PR #4), and the
source-agnostic overlapping-batch feeder (PR #5) are all on `main`. This branch adds **Step 2(a): the
Kafka source** (`glue.KafkaSource`) — the watermark-flavor `RecordSource` for the parallel-batch
feeder. Implemented + unit-tested; broker end-to-end and the RabbitMQ→Kafka bridge remain. Ask the
user before pushing.

## Current branch work (`bem_kafka_source`)

New assets:
- `glue.OffsetWatermark` — durable, out-of-order-safe **contiguous-completed-prefix** offset tracker.
  The engine's K workers `commit` out of order, so a monotonic cursor may only advance over the
  contiguous prefix; this holds a completed-but-behind-a-gap range until the gap fills, then sweeps
  and persists a double-buffered checkpoint (`offset-<topic>-0` + `.bak`).
- `glue.KafkaSource` — watermark `RecordSource` over **ONE unpartitioned topic**; read parallelism is
  `minPartitions` (not Kafka partitions, so records aren't grouped by a resolution key). `nextChunk`
  claims a **count-bounded** `[cursor, min(cursor+recordsPerBatch, latest))` range; `commit` advances
  the watermark; `reclaim` is a no-op (restart re-reads the committed offset; replay = no-op re-add).
- `ParquetParallelFeeder` gains `source=kafka` (args: `bootstrapServers` / `topic` / `checkpoint` /
  `startingOffset` cold-start-only / `minPartitions`).
- `build.sbt` — `spark-sql-kafka-0-10` added as **`Provided`** (supply at launch via `--packages`;
  brings `kafka-clients` transitively for the driver-side `endOffsets` call).

Uncommitted docs this session: `docs/PARALLEL_BATCH_FEEDER.md` §Step 2 (Kafka built),
`.claude/faqs/deployment/kafka-source.md` (new), `.claude/faq_server.py` (instructions string),
`CHANGELOG.md`, `STATUS.md` + `NEXT_STEPS.md`.

## Tests

`sbt test` — **115 unit tests** green (102 prior + `OffsetWatermarkSpec` 7 + `KafkaSourceSpec` 6).
`OffsetWatermarkSpec` covers the out-of-order / gap / idempotency / durability / restart invariants on
a real local FS (no mocks); `KafkaSourceSpec` covers the count-bounding + bounds round-trip. Broker
end-to-end (`boundaryOffset` / `readRange`) is an `IntegrationTest`, like `EngineIT` (needs a live
Kafka) — not in the unit gate.

## Known gaps / remaining Step 2

1. **RabbitMQ→Kafka bridge** — read RabbitMQ → produce to the topic, **throttled so the Spark consumer
   stays ≤ ~5M records behind** (`latestOffset − committedOffset` cap). Kafka as the durable buffer.
2. **`DeltaSource`** — Delta table version / CDF watermark; the same seam, cleanest on Databricks.
3. **Broker end-to-end IntegrationTest** for `KafkaSource` (produce → feed → verify) — needs a Kafka
   broker (embedded or compose), like `FatJarIT`'s Docker approach.
4. Pre-existing: `.142`-Rust same-host baseline (host-asymmetry confound); `SparkRecordOps` 3-jobs/batch;
   multi-node cluster validation; MSSQL/MySQL DDL; `reinitialize()` concurrency cert.

## Uncommitted state

This session's tree changes are pending review before the push (new sources + specs + the docs listed
above). Global rule: **ask the user before committing/pushing.** The `.142` fleet deployment (running
the merged parallel feeder against the live g2 DB) is unchanged and separate; its run state lives in
the dbperf_test project's STATUS/NEXT_STEPS.
