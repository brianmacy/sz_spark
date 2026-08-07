# Project Status

**Date:** 2026-08-07
**Branch:** bem_step2_complete (not yet pushed / no PR)
**State:** M0–M16 + streaming ingest (PR #4) + the overlapping-batch feeder (PR #5) + the Kafka source
(PR #6) are on `main`. This branch **completes Step 2** of the parallel-batch feeder: the RabbitMQ→Kafka
bridge, the Delta source, and a broker end-to-end IntegrationTest. Implemented + unit-tested. Ask the
user before pushing (autonomous mode: proceeding through prep → PR → merge-on-green per standing
direction).

## Current branch work (`bem_step2_complete`)

- `glue.MqToKafka` (Step 2b) — RabbitMQ→Kafka bridge, plain-JVM competing consumer (analog of
  `MqToParquet`). **Produce-THEN-ack** write-ahead; **throttled** so the Spark consumer stays ≤ `maxLag`
  (default 5M) behind (`lag = latestKafkaOffset − committedOffset`, committed read from the feeder's
  checkpoint). Kafka retention + this cap replace unbounded queue growth.
- `glue.DeltaSource` (Step 2c) — watermark seam over a Delta table's Change Data Feed; cursor = table
  version, reusing `OffsetWatermark`. Prereqs: CDF enabled + a STRING `value` column. ⚠ version-granular
  (not row-count) — prefer Kafka for tail-freeness. `delta-spark` added Provided.
- `glue.KafkaSourceIT` — broker end-to-end IntegrationTest (produce → `KafkaSource` → verify ranges +
  projection + watermark). Tagged `IntegrationTest`; `SZ_IT=1 SZ_KAFKA_BOOTSTRAP=<broker>`.
- `ParquetParallelFeeder` gains `source=delta`. `kafka-clients` 3.9.1 added **bundled** (standalone
  bridge runs from the FAT jar; driver-side `endOffsets` needs no `--packages`).

Uncommitted docs this session: `docs/PARALLEL_BATCH_FEEDER.md` §Step 2, `.claude/faqs/deployment/kafka-source.md`,
`CHANGELOG.md`, `STATUS.md`, `NEXT_STEPS.md`.

## Tests

`sbt test` — **122 unit tests** green (115 prior + `MqToKafkaSpec` 3 + `DeltaSourceSpec` 4). The
`KafkaSourceIT` compiles and is excluded from the unit gate (needs a live broker). Real-infra
end-to-end (Kafka broker, RabbitMQ, Delta table) is IntegrationTest-only — same convention as `EngineIT`.

## Step 2 — COMPLETE

(a) KafkaSource ✅ (PR #6) · (b) RabbitMQ→Kafka bridge ✅ · (c) DeltaSource ✅ · broker e2e IT ✅
(scaffolded, runs with live infra). The parallel-batch feeder now supports `inbox` / `kafka` / `delta`.

## Remaining (beyond Step 2)

1. **Run the e2e ITs on live infra** — `KafkaSourceIT` against a real broker; a Delta CDF e2e; a
   RabbitMQ→Kafka bridge e2e. Then wire the Kafka path into a fleet arm and compare to the parquet path.
2. **`.142`-Rust same-host baseline** — remove the ~9% host-asymmetry confound from the parity number.
3. **`SparkRecordOps` 3-jobs/batch** → one-pass sink (minor; DB-bound arm hides it).
4. Pre-existing: multi-node cluster validation; MSSQL/MySQL DDL; `reinitialize()` concurrency cert.

## Uncommitted state

This session's tree changes are pending review before the push (new sources + specs + IT + the docs
above). The `.142` fleet deployment (merged parallel feeder against live g2) is unchanged and separate;
its run state lives in the dbperf_test project's STATUS/NEXT_STEPS.
