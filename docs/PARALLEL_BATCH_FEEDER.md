# Design: overlapping-batch parallel feeder (`glue.ParquetParallelFeeder`)

**Status:** Stage 2 **alternative** to [`ParquetStreamFeeder`](RABBITMQ_INGEST.md) — implemented +
unit-tested (claim/dispose/reclaim, concurrency-safe sinks, no-drop-on-failure, and the
no-head-of-line-block guarantee). Same parquet inbox, same [`AddCore`](../src/main/scala/com/senzing/spark/core/AddCore.scala),
same dead-letter contract ([`DEAD_LETTER.md`](DEAD_LETTER.md)); it replaces **only** how shards are fed
to the engine. Kept **alongside** the streaming feeder, not a replacement of it.

## Why — the micro-batch straggler tail
Structured Streaming (`ParquetStreamFeeder`) commits micro-batches **strictly sequentially**: a batch
finishes only when its **slowest** partition finishes, then the checkpoint advances and the next batch
starts. A few huge-entity records in one shard therefore idle every other slot until they complete —
measured on `.142` at **76% host idle, 37/168 engine threads active** mid-tail. Making shards smaller
or more numerous only *shrinks* the tail; the per-batch barrier remains.

## The fix — overlapping batches, no barrier
Run **K driver threads**, each owning ONE shard end-to-end: **claim → read → `AddCore` → sinks →
dispose**. A slow shard holds a single thread while the other K−1 keep claiming new shards, so a freed
slot refills immediately. The only idle is genuine end-of-input. There is no batch barrier, so there is
no tail.

Each inbox shard is one parquet file ⇒ one Spark partition ⇒ one task ⇒ one core. So **K concurrent
jobs fill K cores — set `concurrency` = the engine slot count** (168 on `.142`). `spark.scheduler.mode=FAIR`
(set by the job) + a distinct scheduler pool per worker share the cluster fairly across the K jobs.

## Ownership without a lock (per-unit dispose)
The parquet inbox files *are* the durable cursor, so no external coordination is needed — a filesystem
atomic rename is the mutual exclusion:

| step | action |
|---|---|
| **claim** | `rename(inbox/part-X.parquet → processing/part-X.parquet)`. The rename is atomic; a losing racer / already-moved shard gets `false` and the caller tries the next. Serialized behind one `synchronized` claimer (the "single reader") that buffers a listing and refills from the inbox as it drains. |
| **process** | read the one shard file → `AddCore.run(spark, ds, runId, staging/<unit>)` into a **per-unit** staging dir → `SplitResult(good, errors)`. |
| **sinks** | write each unit's `errors`/`good` as their OWN atomically-renamed single files (`de-*.parquet` / `af-*.parquet`). |
| **dispose** | archive the shard (rename into `archive/`) if `archive` is set, else delete it; then drop its staging dir. |
| **reclaim** | on start, move any shard left in `processing/` (a prior run's in-flight work) back to the inbox **before** claiming. |

### ⛔ Two concurrency hazards a naive port of `ParquetStreamFeeder` would hit
1. **Shared staging clobber.** [`SparkRecordOps.run`](../src/main/scala/com/senzing/spark/core/SparkRecordOps.scala)
   writes its engine pass to `stagingPath` with `SaveMode.Overwrite`. K concurrent jobs sharing one
   staging path would overwrite each other. ⇒ **per-unit staging** `staging/<unit>` (`<unit>` = the
   shard's `part-<uuid>` basename).
2. **Append committer race.** `ParquetStreamFeeder.writeSinks` uses `SaveMode.Append`; concurrent Append
   jobs race on the `_temporary`/`_SUCCESS` commit protocol. ⇒ each unit writes its sinks as **unique
   single files via atomic rename** ([`ShardIo.writeSingleFile`](../src/main/scala/com/senzing/spark/glue/ShardIo.scala)),
   never Append. The dead-letter/output dirs stay flat collections of parquet files (the same shape the
   drainer produces for the inbox), so the row schema — and therefore
   [`DeadLetterReprocess`](../src/main/scala/com/senzing/spark/glue/DeadLetterReprocess.scala) and the
   downstream dedup keys — are **unchanged**.

## Correctness / semantics
- **At-least-once, never drop.** A shard whose processing **throws** is left in `processing/` and
  reclaimed + reprocessed on the next restart. Re-adding an already-resolved record is a fast optimized
  no-op (idempotent `addRecord` on `DATA_SOURCE,RECORD_ID`), so replay is cheap. A crash between
  add-committed and dispose ⇒ the shard is reclaimed and re-added (no-op). No shard is lost.
  - *(A transient failure waits for a restart to retry, rather than looping in-process — this keeps a
    poison shard from hot-looping. In-process retry with backoff is a possible refinement.)*
- **Engine concurrency** is the existing per-executor-JVM singleton under the read lock (write lock only
  for config-drift reinit) — K concurrent tasks share it exactly as the streaming feeder's within-batch
  partitions already do.
- **Memory-bounded:** at most K × `shardRecords` records in flight (a fixed thread pool); references are
  dropped per unit.
- **Random shard order only** — never partitioned by a resolution key (a resolution-key grouping causes
  lock contention). Shards are claimed in listing order; their *contents* are whatever the drainer wrote.

## Job args (`glue.ParquetParallelFeeder`)
| Arg | Meaning | Default |
|---|---|---|
| `inbox` | parquet shard dir (claim source) | — |
| `processing` | dir shards are claimed into while in flight (reclaimed on restart) | — |
| `archive` | if set, disposed shards are moved here; empty ⇒ deleted | `""` (delete) |
| `staging` | base dir for per-unit `AddCore` staging (`staging/<unit>`) | `staging` |
| `deadLetter` | durable **DLQ dir** for each unit's `errors` frame (empty ⇒ no write) | `""` |
| `output` | append-only affected-entity **change-feed dir** for the `good` frame (empty ⇒ no write) | `""` |
| `concurrency` | K worker threads = concurrent shards in flight; set ≈ engine slot count | `32` |
| `trigger` | `default` (long-running, poll forever) or `availableNow` (drain then exit) | `default` |
| `emptyMs` | idle window before `availableNow` exits | `30000` |
| `runId` | ties affected-entity rows to a run | `run` |

`inbox`, `processing`, `archive`, `staging`, and the sink dirs must all live on storage reachable by the
drainer and every Spark node (a shared volume) — same constraint as the streaming feeder.

## Launch (replacing the streaming feeder on the same inbox)
```bash
spark-submit --class com.senzing.spark.glue.ParquetParallelFeeder \
  --conf spark.scheduler.mode=FAIR \
  sz-spark-assembly.jar \
  inbox=/data/tmp/sz_spark_io/inbox \
  processing=/data/tmp/sz_spark_io/processing \
  archive=/data/tmp/sz_spark_io/archive \
  staging=/data/tmp/sz_spark_io/staging \
  deadLetter=/data/tmp/sz_spark_io/deadletter \
  output=/data/tmp/sz_spark_io/affected \
  concurrency=168 trigger=default runId=sayari
```
The [Stage-1 drainer](RABBITMQ_INGEST.md) (`MqToParquet`, ack-on-persist) is **unchanged** — it remains
the RabbitMQ→inbox adapter. Only Stage 2 changes.

## Measuring the win (see [`PERFORMANCE.md`](PERFORMANCE.md))
MQ ack rate reflects the drainer's persist rate, not ER throughput — do **not** judge on it. Judge on:
- **`.142` host idle%** (target: 76% → low) and **engine-thread duty cycle** (should stay high with no
  tail dips), grounded against the run log;
- a deliberately slow shard must **not** idle the other slots (the tail-gone property — asserted in
  `ParquetParallelFeederSpec`);
- DB-side resolved-row deltas for completion.

Wall-clock / throughput is noisy on a shared, growing DB, so the verdict is idle% + duty cycle + tail
behavior, not a raw rec/s delta.

## What Step 2 adds (not in this doc)
A **source seam** so the same overlapping-batch driver runs over Kafka (offset cursor, monotonic
watermark commit) and Delta (version cursor), plus a throttled RabbitMQ→Kafka bridge. The per-unit
dispose flavor here is one of two commit flavors; Kafka/Delta use the watermark flavor. Kept out of
Step 1 to land the working RabbitMQ path first.
