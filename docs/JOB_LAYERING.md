# Design: core (engine) jobs vs glue (transport) jobs

**Status:** the `core/` + `glue/` split is implemented (`AddCore`, `ParquetStreamFeeder`,
`DeadLetterReprocess`); remaining rows below are the intended shape. Pairs with
[`RABBITMQ_INGEST.md`](RABBITMQ_INGEST.md).

## Principle
Split the job layer in two, so the Senzing/ER work never knows where records came from or go:

- **CORE jobs** — engine-facing, **transport-agnostic**. Input: a `Dataset[InputRecord]` (records to
  process) or an ID set. Output: `Dataset[AffectedEntityRow]` good-frame + `Dataset[ErrorRow]`
  error-frame. They call the engine (`addRecord` / `processRedoRecord` / `searchByAttributes` /
  `getEntity`) via the existing `SparkRecordOps.run(..., engineWorkerFactory(op, verb))`. **No AMQP,
  no file formats, no I/O transport.**
- **GLUE jobs** — transport adapters, **no engine**. Move records between a transport and the
  interchange format. E.g. MQ→parquet, parquet→(hand to a core job), JSONL→parquet, shard move.

## Interchange format = Parquet
Glue produces/consumes **parquet** `InputRecord` shards; core reads/writes parquet frames. Parquet is
the seam. This is why the RabbitMQ ingest is two-stage: the glue (`MqToParquet`) buffers to parquet and
acks on persist; a core add job consumes parquet — decoupling MQ ack latency from ER latency.

## Job taxonomy

### Core (engine-facing, transport-agnostic)
| Core | Verb | Input | Output |
|---|---|---|---|
| `AddCore` (consumer/add) | `addRecord` WITH_INFO | `Dataset[InputRecord]` | affected + error |
| `SearchCore` (search) | `searchByAttributes` | `Dataset[InputRecord]` (attr JSON) | request+result + error |
| `GetCore` (get) | `getEntity`/`getEntityByRecordId` | `Dataset` of IDs | entity JSON + error |
| `RedoCore` (redoer) | `processRedoRecord` WITH_INFO | **engine redo queue** (self-sourced) | affected + error |

`RedoCore` is the intentional exception: its "source" is the engine's own `SYS_EVAL_QUEUE`
(`getRedoRecord`, driver-side), not an external transport — so it has **no glue** and stays as the
current `RedoJob` shape. Everything else takes a DataFrame from glue.

### Glue (transport adapters, no engine)
| Glue | Direction | Notes |
|---|---|---|
| `MqToParquet` (`RabbitMqSource`) | RabbitMQ → parquet inbox | consume → persist shard → **then ack** (write-ahead); see RABBITMQ_INGEST.md |
| `JsonlToParquet` | JSONL file → parquet | so file loads also enter via the parquet seam (parse RECORD_ID) |
| `ParquetStreamFeeder` | parquet inbox → `AddCore` | long-running `readStream` → `foreachBatch(AddCore.run)`; checkpoint + `cleanSource=archive`; persists the `SplitResult` to the **dead-letter** + **output** sinks (no per-batch repartition) |
| `DeadLetterReprocess` | dead-letter dir → re-feed inbox | reads the quarantined `errors` shards, keeps the reprocessable categories, re-emits them as `InputRecord` shards; see [`DEAD_LETTER.md`](DEAD_LETTER.md) |
| (future) `ParquetToMq`, sink adapters | as needed | |

### Diag (diagnostics, engine-adjacent, no ER output)
| Diag | Mechanism | Notes |
|---|---|---|
| `StatsPlugin` / `StatsSampler` | Spark `SparkPlugin` (executor + driver) | enabled by `--conf spark.plugins=…diag.StatsPlugin`; one sampler thread per executor JVM calls the engine's reset-on-read `getStats()` on a cadence and ships each sample to the driver log under the `SZ_STATS` prefix. Zero code path when not listed. See [`PERFORMANCE.md`](PERFORMANCE.md) §Monitoring. |

## How the existing jobs refactor into this
- `AddUpdateJob` (today = read JSONL **glue** + add **core** mashed together) → **`AddCore`** (pure) +
  a thin glue main (`ParquetFeeder`/`JsonlToParquet`) that supplies the DataFrame. Same for
  `DeleteJob`/`SearchJob`.
- `RedoJob` → `RedoCore` (unchanged behavior; already engine-sourced).
- `RabbitMqSource` (from RABBITMQ_INGEST.md) is a **glue** job (`MqToParquet`) — it has no engine at all.
- `SparkRecordOps` / `RecordJob.engineWorkerFactory` / `RecordWorker` are already the core substrate;
  this refactor mostly moves the *input reading* out of the `*Job` objects into glue and leaves a thin
  core entry point that takes `Dataset[InputRecord]`.

## Why this matters here (the reference A/B)
- The Spark loader = **glue `MqToParquet`** (drains the same queue as the reference consumer
  fleet, MQ-exclusive split) → **core `AddCore`** (from parquet) + **core `RedoCore`** (engine queue). A
  reference consumer on one app host vs Spark on the other, same shared database.
- Clean layering also makes the A/B honest: the core is identical regardless of transport, so a
  transport swap (file vs MQ) can't quietly change the ER path.

## Decisions (2026-08-06)
1. **Package layout: `core/` + `glue/`.** `com.senzing.spark.core.*` = AddCore/SearchCore/RedoCore
   (transport-agnostic, DataFrame in → frames out). `com.senzing.spark.glue.*` = MqToParquet /
   JsonlToParquet / ParquetFeeder (transport adapters, no engine), each with a thin `main` that wires a
   transport to a core.
2. **Stage-2 feeder is a LONG-RUNNING Structured Streaming query** (revised 2026-08-06 after the Fable
   Databricks-naturalness review, reversing the earlier one-shot choice): `readStream.format("parquet")`
   → `foreachBatch(AddCore.run)` with checkpoint + `cleanSource=archive`. A one-shot per invocation
   re-pays per-JVM native/engine init every run and diverges from an always-on reference consumer fleet; a persistent
   query amortizes init and makes the A/B fair. `Trigger.AvailableNow` kept for scheduled-batch use. (The
   MQ→parquet Stage-1 drainer remains a standalone always-on plain-JVM process, not a Spark job.)
3. **`GetCore` deferred** — not needed for the load A/B (affected-entity IDs suffice). Build
   AddCore/RedoCore + glue first; add `GetCore` when settled-entity content is actually required.
