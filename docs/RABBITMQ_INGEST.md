# Design: RabbitMQ ingest source for sz_spark

**Status:** Stage 2 (the Structured Streaming feeder, `glue.ParquetStreamFeeder`) is **implemented +
unit-tested**, including the dead-letter/output sinks (see [`DEAD_LETTER.md`](DEAD_LETTER.md)); the
Stage 1 drainer is a standalone plain-JVM consumer (design below). The shared streaming feeder has been
**run at multi-node scale on the on-prem fleet** as part of the Kafka ingest cutover (the same
`ParquetStreamFeeder`, fed from a bridged queue); the **RabbitMQ Stage-1 drainer specifically** has not
yet been exercised at fleet scale.
**Goal:** let Spark consume Senzing JSON records off a **RabbitMQ** queue and `addRecord` them into your
datastore. The Spark loader runs as a **competing consumer** — it can run alongside any other consumers
on the same queue, and RabbitMQ's exclusive per-message delivery partitions the work across all of them
with **zero coordination and no dedup**. All consumers `addRecord` into the same database.

## The insight: source and processor are already separated
Every sz_spark data job is `source → Dataset[InputRecord] → shared processor`, where
`InputRecord = (dataSource, recordId, payloadJson)` and the processor is
`SparkRecordOps.run(spark, input, staging, RecordJob.engineWorkerFactory(op, runId, verb), …)`.
Only the **source** and the **verb** differ:

| Job | Source | Verb |
|---|---|---|
| `AddUpdateJob` | `RecordJob.readRecords(path)` — `spark.read.text` JSONL | Add |
| `RedoJob` | `RedoSource.drainBatch(getRedoRecord)` — driver-side engine-queue pull | Redo |
| **RabbitMQ ingest** (new) | **executor-side AMQP → parquet inbox → streaming feeder** | **Add** |

So the RabbitMQ path reuses the entire processing/engine/config-drift/error/parquet pipeline
unchanged; it only supplies records from AMQP instead of a file.

## ⛔ Driver-side ack is IMPOSSIBLE — two hard RabbitMQ constraints (this drives the whole design)
Both "ack from the driver" options are dead:
1. **An ack must be issued on the same channel that received the delivery.** If the driver pulls and
   executors process, the executors cannot ack (wrong channel/connection) and the driver cannot ack
   work it did not receive-and-hold.
2. **`consumer_timeout`.** An unacked delivery held past the broker timeout gets the channel
   force-closed and the messages requeued. A Spark stage sitting between a driver pull and a driver
   ack would trip it.

⇒ The channel + `basic_qos` prefetch + ack must live **inside the executor, co-located with
`addRecord`** — the same MQ management a typical single-process Senzing RabbitMQ consumer does: **one
`Connection`+`Channel`, `basic_qos(prefetch = threads)`, and ack/reject only on the channel-owning
task**. That works because the ack is in-process and each in-flight (unacked) message is being actively
worked, so it clears the `consumer_timeout` comfortably.

Note also: `RedoSource`'s driver-side single-consumer choice is about the *engine* redo queue
("undemonstrated concurrent multi-consumer `getRedoRecord()`") — NOT applicable here; competing
consumers on one AMQP queue is idiomatic. But the ack constraints above, not throughput, are why the
consumer must be executor-side (or, as chosen below, an out-of-Spark drainer).

### Consequence: decouple ingest from ER with a durable parquet buffer
Rather than fight the ack/timeout coupling inside one job, split it into two stages joined by a
durable **parquet inbox**. Ack is then gated only on a *fast* persist, never on the slow `addRecord`.

## Primary design — two-stage: durable parquet buffer (RECOMMENDED)

### Stage 1 — `RabbitMqSource` (user code): MQ → parquet, ack-on-persist
A consumer that owns the AMQP channel and does only fast work:
1. Open `Connection`/`Channel` to `SZ_AMQP_URL` (long `heartbeat`), `basicQos(prefetch)` sized to the
   persist-batch.
2. Consume a batch of deliveries (bodies = Senzing JSON), buffering `(deliveryTag, body)`.
3. **Persist the batch to a parquet shard written as `inbox/.tmp-<uuid>`, then atomically `rename` it
   into `inbox/part-<uuid>.parquet`** — atomic appearance so the streaming feeder (below) never lists a
   half-written Parquet footer. Flush/durable before the rename.
4. **Only then `basicAck`** the batch's delivery tags **on the same channel**. Loop.

WARNING — the load-bearing invariant: **persist-THEN-ack (write-ahead).**
- Crash after persist, before ack → RabbitMQ redelivers → duplicate parquet row → idempotent
  `addRecord` (on `DATA_SOURCE,RECORD_ID`) absorbs it downstream. Safe.
- Crash before persist → unacked → redelivered. Safe.
- **Ack-before-persist would silently DROP records — never do it.**

Because Stage 1 acks right after a fast parquet write (not after ER), both RabbitMQ constraints are
satisfied trivially: ack is on the receiving channel, and it is well within `consumer_timeout`.
Prefetch bounds unacked to one persist-batch. Stage 1 is a **plain JVM consumer** running next to
RabbitMQ (outside Spark), writing parquet.

### Stage 2 — Structured Streaming feeder (`glue.ParquetStreamFeeder`) → `core.AddCore`
Do **not** hand-roll shard tracking (move-to-`done/`/markers) — that reinvents Spark's checkpointed file
source. Use the **Spark file streaming source** (portable OSS):
```scala
spark.readStream.format("parquet").schema(inputRecordSchema)
     .option("maxFilesPerTrigger", N).option("cleanSource", "archive")
     .option("sourceArchiveDir", archive).load(inbox)
     .writeStream.option("checkpointLocation", ckpt)
     .foreachBatch { (df, _) =>
        val result = core.AddCore.run(spark, df, runId, stagingPath)  // SplitResult(good, errors)
        writeSinks(result, deadLetter, output)                        // durable DLQ + change-feed
     }
     .start()   // default micro-batch, LONG-RUNNING
```
- **Long-running micro-batch query** (not a fresh JVM per batch): a fresh executor JVM per invocation
  would re-pay native self-extraction + `SzEnvironment` build + DB-connection setup on *every* run; a
  single persistent Structured Streaming query amortizes that engine/native init across the whole run
  (the reason the engine is per-JVM). `Trigger.AvailableNow` remains available for scheduled-batch use.
- **Checkpoint = exactly-once file feeding** (each file feeds exactly one committed batch; never re-fed).
  `cleanSource=archive` replaces move-to-`done/`. `maxFilesPerTrigger` gives backpressure.
- **NO per-batch repartition.** The file source's own partitions feed the executor slots directly, so
  read + `add_record` **fuse into one pipelined stage**. An earlier `repartition(N)` inside
  `foreachBatch` was pure overhead (a shuffle that idled slots, and could reduce parallelism below the
  file count) and was removed — measured ~0.1% of batch wall, closed no gap. See
  [`PERFORMANCE.md`](PERFORMANCE.md). (Random partitioning of the *input shards themselves* is still
  correct — never partition by a resolution key.)
- **`foreachBatch` is the natural home for the exactly-once side-effect pass:** each micro-batch hands
  `AddCore.run` a plain DataFrame; the `mapPartitions` + read-lock engine bracket run unchanged inside it.
  Semantics identical to the batch jobs: file→batch is exactly-once, a failed batch re-runs, engine
  mutations stay at-least-once absorbed by idempotent `addRecord`. `spark.speculation=false` carries over.
- **Durable failure capture:** the `SplitResult` is no longer discarded — `errors` → the `deadLetter`
  dir (DLQ), `good` → the `output` change-feed dir, both `SaveMode.Append`, both opt-in. Full contract in
  [`DEAD_LETTER.md`](DEAD_LETTER.md).
- **Databricks variant (one glue file):** same query with `format("cloudFiles")` (**Auto Loader**,
  `cloudFiles.format=parquet`), checkpoint/schema location on a **UC Volume**, output/dead-letter frames as
  **Delta**; run as a Databricks Job. Auto Loader is DBR-proprietary → glue-only, never the portable core.

### Correctness / semantics (two-stage)
- **Durability boundary = parquet.** Once a record is in a shard it is off the MQ and safe; ER failures
  land in the error frame (→ dead-letter dir) and are retryable **from parquet**, no MQ redelivery needed.
- **At-least-once end-to-end**, dedup by idempotent `addRecord`. Both stage boundaries re-process on
  crash, never drop.
- Engine concurrency (Stage 2): one engine per executor JVM under the **read** lock (write lock only for
  config-drift reinit) — concurrency = `spark.executor.cores`.
- Failure taxonomy reused (`ErrorTaxonomy`/`Backoff`/`CircuitBreaker`).
- **What the MQ ack rate measures:** with the two-stage design, MQ ack rate reflects the Stage-1
  **persist** rate, NOT end-to-end ER throughput. To gauge actual load progress, watch **engine-thread
  duty cycle** and per-record counters (see [`PERFORMANCE.md`](PERFORMANCE.md)), or DB-side
  resolved-row deltas.

## Alternative — inline executor-side competing consumers (NOT recommended for now)
Each `mapPartitions` task owns its own channel, `consume → addRecord → ack` inline, prefetch small
(2–4), bounded by quota/idle-timeout, re-run on a schedule. Mirrors a typical single-process RabbitMQ
consumer most closely and avoids the intermediate parquet, but couples ack to ER latency (per-record processing must stay well
under `consumer_timeout`) and makes the job a long-running consume loop rather than a batch job. Keep as
a fallback; the two-stage buffer above is simpler to get correct.

## Redo is a SEPARATE, already-built concern
Two different queues — do not conflate:
- **RabbitMQ queue** = input records → consumed by the Spark feeder (and any other consumers on the
  queue).
- **Engine redo queue** = `SYS_EVAL_QUEUE` in the database, generated by every `addRecord`. It is
  **global in the database** and already handled by the existing **`RedoJob`** (driver-side
  `getRedoRecord` drain → parallel `processRedoRecord`). **Reuse `RedoJob` unchanged** and run it on a
  schedule. It drains the shared DB redo queue safely (PG `FOR UPDATE SKIP LOCKED`), so multiple redoers
  can drain concurrently. **No new redo code.**

## Job args
### Stage 1 — `RabbitMqSource`
| Arg | Meaning | Default |
|---|---|---|
| `amqpUrl` | `amqp://user:pass@<mq-host>:5672/%2F` (or env `SZ_AMQP_URL`) | — |
| `queue` | queue name | `senzing-rabbitmq-queue` |
| `inbox` | output dir for parquet shards | — |
| `prefetch` | `basicQos` (bounds unacked ≈ one persist-batch) | 5000 |
| `shardRecords` | records per parquet shard before persist+ack | 5000 |
| `emptyMs` | idle-timeout to stop draining | 30000 |

### Stage 2 — `glue.ParquetStreamFeeder` (Structured Streaming → `core.AddCore`)
| Arg | Meaning | Default |
|---|---|---|
| `inbox` | parquet shard dir (`readStream` source) | — |
| `checkpoint` | checkpoint location (exactly-once file tracking) | — |
| `archive` | `sourceArchiveDir` for `cleanSource=archive` | — |
| `staging` | `AddCore` transient staging path | — |
| `maxFilesPerTrigger` | files/micro-batch (backpressure) | 200 |
| `trigger` | `default` (long-running) or `availableNow` | `default` |
| `deadLetter` | durable **dead-letter (DLQ) dir** for the per-batch `errors` frame, `SaveMode.Append` (empty ⇒ no write) | — |
| `output` | append-only affected-entity **change-feed dir** for the `good` frame (empty ⇒ no write) | — |

> There is **no** `partitions` / per-batch repartition arg — it was removed as dead weight (above).

Each record supplies **both** its `DATA_SOURCE` and its `RECORD_ID` in the JSON body; both are parsed
from the body (exactly as `RecordJob.readRecords` does) — there is no configured/launch `dataSource`.
A record missing either key is minted with the empty key(s) and dead-lettered as `BAD_INPUT` at the
engine seam, never silently stamped.

**Failed records are captured, not dropped.** `AddCore.run` returns `SplitResult(good, errors)`; the
feeder persists `errors` to `deadLetter=` (the DLQ equivalent of a RabbitMQ dead-letter queue) and
`good` to `output=`. Reprocess with `glue.DeadLetterReprocess`. Full contract,
the terminal-vs-reprocessable category split, and the Databricks **Delta quarantine table** variant
are in **[`DEAD_LETTER.md`](DEAD_LETTER.md)**.

## Dependency
`com.rabbitmq:amqp-client` (**Apache-2.0** ✓). **Bundled** (not `Provided`) — the cluster does not
ship it. Pin to latest 5.x; add to `build.sbt` `libraryDependencies`.

## Cluster / build notes
- The staged Spark dist and the build target should match; bump `sparkVersion` to the staged dist's
  version and rebuild (Scala 2.13 unchanged, low risk under `Provided`) so the jar matches the cluster.
- ⚠ sz_spark has **never run on a real multi-node cluster** (its own STATUS gap #1). First cluster
  bring-up (Spark standalone master+workers, `SENZING_ENGINE_CONFIGURATION_JSON` → the DB, native
  self-extract via `LD_LIBRARY_PATH=$SENZING_EXTRACT_DIR/<sha>/lib`) is first-time and its own risk.
  Smoke with `SelfCheck` on one node first.
- Engine build: run the **same engine build on every node** — a mismatch between nodes changes ER
  behavior and makes results inconsistent.

## Engine config (when attaching to an existing datastore)
`SENZING_ENGINE_CONFIGURATION_JSON` →
`{"SQL":{"CONNECTION":"postgresql://USER:PASS@<db-host>:5432:<db>"}}`
plus the same engine settings the existing loaders use, so both write compatibly.
**Do NOT run `InitJob`** when the schema, config, and records already exist — only initialize a fresh
datastore.

## Monitoring progress
The MQ ack rate reflects the Stage-1 persist rate, not end-to-end ER throughput (Stage 1 acks on
persist, well before the record resolves). To watch actual load progress:
- **engine-thread duty cycle** and per-record counters (see [`PERFORMANCE.md`](PERFORMANCE.md)); and
- **DB-side resolved-row deltas**, which confirm completion.

Per-consumer ack rate is available via RabbitMQ per-consumer stats if you want to see each consumer's
Stage-1 throughput separately.

## Design summary
- Stage 1 = **plain JVM consumer run OUTSIDE Spark**, next to RabbitMQ (running it as a Spark job wastes
  a cluster and re-imports the `consumer_timeout` coupling). Writes parquet, tmp-then-rename,
  persist-then-ack.
- Stage 2 = **long-running Structured Streaming file-source feeder + `foreachBatch(AddCore)`** (not a
  hand-rolled one-shot), **no per-batch repartition**, with durable dead-letter/output sinks.
  `cleanSource=archive`, checkpoint. Auto Loader is the DBR-only glue variant.
- Interchange = **plain Parquet** for the transient inbox; Delta only for the output/dead-letter frames
  on Databricks.

## Configuration to confirm for your deployment
1. `shardRecords` / persist-batch size (parquet shard granularity vs ack cadence).
2. `inbox` / `checkpoint` / `archive` must live on storage reachable by **both** the Stage-1 drainer and
   **all** Spark nodes (a shared volume or object store) — confirm the path is mountable everywhere.

> **Databricks version note:** target **DBR 17.x** (Spark 4.0 / Scala 2.13); sz_spark needs
> `LD_LIBRARY_PATH` + cluster init scripts, so use **classic dedicated (single-user) clusters** —
> serverless is out — and pin the cluster size for a long-running query.
