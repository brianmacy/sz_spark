# Design: RabbitMQ ingest source for sz_spark

**Status:** Stage 2 (the Structured Streaming feeder, `glue.ParquetStreamFeeder`) is **implemented +
unit-tested**, including the dead-letter/output sinks (see [`DEAD_LETTER.md`](DEAD_LETTER.md)); the
Stage 1 drainer is a standalone plain-JVM consumer (design below). Not yet run on a multi-node
cluster.
**Goal:** let Spark executors consume Senzing JSON records off a **RabbitMQ** queue and `addRecord`
them into the shared datastore, so a Spark loader can run as a *competing consumer* alongside a Rust
`sz_rabbit_consumer` fleet on the **same queue** and **same DB**.

## Motivating deployment (the perf rig)
- A publisher feeds the full **~1B-record corpus** into one RabbitMQ queue
  (`senzing-rabbitmq-queue`).
- **App host A** keeps the **Rust** consumer fleet.
- **App host B** stops its Rust fleet and runs the **Spark** feeder instead.
- Both pull the **same** queue → RabbitMQ's exclusive per-message delivery partitions the work with
  **zero coordination and no dedup**. Both `addRecord` into the same PostgreSQL database.
- Net: a clean **Rust-vs-Spark same-DB loader A/B**. (MQ has ample headroom; the ceiling is engine+DB
  add throughput, so this is a per-loader comparison, not an MQ-bound one.) See
  [`PERFORMANCE.md`](PERFORMANCE.md) for the measured outcome — the two loader sources are
  performance-equivalent at the same duty cycle.

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
`addRecord`** — exactly the MQ management the Rust consumer does. Per the FAQ
`architecture/consumer-redoer-concurrency`, the Rust consumer uses **one `Connection`+`Channel`,
`basic_qos(prefetch = threads)`, and ack/reject only on the channel-owning task**; it works because
the ack is in-process and each in-flight (unacked) message is being actively worked, so it clears the
`consumer_timeout` comfortably.

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
- **Long-running micro-batch query** (decision, 2026-08-06, reverses the earlier one-shot): a fresh
  executor JVM per invocation would re-pay native self-extraction + `SzEnvironment` build + DB-connection
  setup against the large pre-existing corpus *every run*; a persistent query amortizes it (the reason the
  engine is per-JVM) and mirrors the Rust fleet's always-on model → **fairer A/B**. `Trigger.AvailableNow`
  remains available for scheduled-batch use.
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
- **Throughput metric shifts:** MQ ack rate now measures Stage-1 persist rate, NOT ER throughput. Compare
  the Rust-vs-Spark A/B by **engine-thread duty cycle** and invariant per-record counters (see
  PERFORMANCE.md), or DB-side by resolved-row deltas — not wall clock on a shared, growing DB.

## Alternative — inline executor-side competing consumers (NOT recommended for now)
Each `mapPartitions` task owns its own channel, `consume → addRecord → ack` inline, prefetch small
(2–4), bounded by quota/idle-timeout, re-run on a schedule. Mirrors the Rust consumer most closely and
avoids the intermediate parquet, but couples ack to ER latency (per-record processing must stay well
under `consumer_timeout`) and makes the job a long-running consume loop rather than a batch job. Keep as
a fallback; the two-stage buffer above is simpler to get correct.

## Redo is a SEPARATE, already-built concern
Two different queues — do not conflate:
- **RabbitMQ queue** = input records → consumed by the Rust fleet + the Spark feeder.
- **Engine redo queue** = `SYS_EVAL_QUEUE` in the shared DB, generated by every `addRecord`. It is
  **global in the database** and already handled by the existing **`RedoJob`** (driver-side
  `getRedoRecord` drain → parallel `processRedoRecord`). We **reuse `RedoJob` unchanged**, run on a
  schedule, exactly as the Rust fleet runs redoers. Spark `RedoJob` and Rust redoers both drain the
  shared DB redo queue safely (PG `FOR UPDATE SKIP LOCKED`). **No new redo code.**

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

Records carry their `DATA_SOURCE` + a hash `RECORD_ID`; `RECORD_ID` is parsed from the body
(as `RecordJob.readRecords` does), and the add is stamped with the configured `dataSource`.

**Failed records are captured, not dropped.** `AddCore.run` returns `SplitResult(good, errors)`; the
feeder persists `errors` to `deadLetter=` (the DLQ equivalent of the Rust consumer's RabbitMQ
dead-letter queue) and `good` to `output=`. Reprocess with `glue.DeadLetterReprocess`. Full contract,
the terminal-vs-reprocessable category split, and the Databricks **Delta quarantine table** variant
are in **[`DEAD_LETTER.md`](DEAD_LETTER.md)**.

## Dependency
`com.rabbitmq:amqp-client` (**Apache-2.0** ✓). **Bundled** (not `Provided`) — the cluster does not
ship it. Pin to latest 5.x; add to `build.sbt` `libraryDependencies`.

## Cluster / build notes (perf rig)
- The staged Spark dist and the build target should match; bump `sparkVersion` to the staged dist's
  version and rebuild (Scala 2.13 unchanged, low risk under `Provided`) so the jar matches the cluster.
- ⚠ sz_spark has **never run on a real multi-node cluster** (its own STATUS gap #1). First cluster
  bring-up (Spark standalone master+workers, `SENZING_ENGINE_CONFIGURATION_JSON` → the DB, native
  self-extract via `LD_LIBRARY_PATH=$SENZING_EXTRACT_DIR/<sha>/lib`) is first-time and its own risk.
  Smoke with `SelfCheck` on one node first.
- Engine build: the app hosts may run a locally **patched** engine (e.g. a cold-start guard + arena
  change) while the smoke host uses the stock dist — record any such mismatch as a **provenance
  confound** for the A/B.

## Engine config (authoritative, from the running fleet)
`SENZING_ENGINE_CONFIGURATION_JSON` →
`{"SQL":{"CONNECTION":"postgresql://senzing:…@<db-host>:5432:<db>"}}`
plus the fleet's engine settings (ADVISORY + deferred-write + RES_ENT.FEATURES — match the fleet).
**Do NOT run `InitJob`** — the schema, config, and pre-existing records already exist.

## Measurement (the A/B)
Prefer **engine-thread duty cycle** and **invariant per-record counters** over wall clock (the DB is
shared and growing — see PERFORMANCE.md §"Measured findings"). For completeness, per-loader ack rate
can be split by consumer tag via RabbitMQ per-consumer stats, and resolved-row deltas confirm
completion.

## Resolved (Fable review, 2026-08-06)
- Stage 1 = **plain JVM consumer run OUTSIDE Spark**, next to RabbitMQ (a Spark job there wastes a
  cluster + re-imports the `consumer_timeout` coupling). Writes parquet, tmp-then-rename, persist-then-ack.
- Stage 2 = **long-running Structured Streaming file-source feeder + `foreachBatch(AddCore)`** (not a
  hand-rolled one-shot), **no per-batch repartition**, with durable dead-letter/output sinks.
  `cleanSource=archive`, checkpoint. Auto Loader is the DBR-only glue variant.
- Interchange = **plain Parquet** for the transient inbox; Delta only for OUTPUT/dead-letter frames on
  Databricks.

## Still open for the user
1. `shardRecords` / persist-batch size (parquet shard granularity vs ack cadence).
2. Split the Spark app host (half Rust / half Spark) or run it **all-Spark**?
3. `inbox`/`checkpoint`/`archive` on a **shared volume both hosts mount** — confirm.

## Doc fix flagged (DATABRICKS.md)
"DBR 14+" is wrong for Spark 4.0/Scala 2.13 ⇒ **DBR 17.x**; sz_spark needs `LD_LIBRARY_PATH`/init scripts
⇒ **classic dedicated (single-user) clusters, serverless is out**; pin cluster size for a long-running query.
