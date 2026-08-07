# Design: source-agnostic overlapping-batch feeder

**Status:** implemented + unit-tested (overlap / no-head-of-line-block, no-drop, per-chunk sinks, and
the `InboxSource` claim/dispose/reclaim). The tail-killing alternative to
[`ParquetStreamFeeder`](RABBITMQ_INGEST.md); same [`AddCore`](../src/main/scala/com/senzing/spark/core/AddCore.scala),
same dead-letter contract ([`DEAD_LETTER.md`](DEAD_LETTER.md)). Entry point: `glue.ParquetParallelFeeder`.

## Why — the straggler tail is fundamental and unpredictable
You can never know ahead of time which records are slow (a huge entity can take orders of magnitude
longer). Structured Streaming commits micro-batches **strictly sequentially**, so a batch ends only
when its slowest task does — every batch is exposed to a straggler idling the whole cluster (measured
on `.142`: 76% idle, 37/168 engine threads active mid-tail). Shrinking batches only shrinks the tail;
the barrier remains. So SS's **execution model** is disqualified (its sources — Auto Loader, Delta,
Kafka — are fine, and reused below).

## The mechanism — partition-level work-stealing over overlapping chunks
Keep **K chunk-jobs in flight** concurrently under `spark.scheduler.mode=FAIR`. A freed executor slot
immediately pulls the next pending **partition** from *any* in-flight chunk, so a slow record holds
exactly **one** slot while the rest keep flowing. Partition-level refill, not batch-level — the tail
never idles the cluster. The per-chunk barrier only bites at true end-of-input.

Per chunk: `df.repartition(P)` (Spark bin-packs the tiny shards; this spreads them evenly across slots)
→ **one** `AddCore.run` (amortizing its staging-write + sink read-backs over the whole chunk) → sinks
once → commit. The driver does **metadata only** (claim/commit/reclaim + job submission); all record
data rides Spark partitions to the executors.

> This replaced a v1 that ran one `AddCore.run` per single 5000-record shard — ~3 Spark jobs *per file*
> — which starved the executors (~4× regression: 166 rec/s, CPU *down*). The unit must be a multi-file
> chunk so the fixed per-`AddCore` overhead amortizes; v1's mistake was a 1-file (degenerate 1-partition)
> unit that pulled the data path onto the driver.

## The source is anything — the seam and two commit flavors
[`RecordSource`](../src/main/scala/com/senzing/spark/glue/RecordSource.scala): `nextChunk(cursor) →
Chunk(bounds, df, nextCursor)`, `commit(bounds)`, `reclaim()`. The engine touches only this seam, so
the parquet inbox is just one adapter. Transactionally-ack'd MQ (RabbitMQ/SQS) map naturally to the
**dispose** flavor; everyone else's cursor-native sources map to the **watermark** flavor.

| Source | Cursor | commit flavor | Portable? |
|---|---|---|---|
| **InboxSource** (RabbitMQ via the drainer) | none | **dispose** — claim `inbox→processing/<id>/` (atomic rename), archive/delete on commit, reclaim moves in-flight back to inbox | **on-prem only** — rename is copy+delete & non-atomic on S3/ADLS/GCS |
| Kafka *(Step 2)* | offset | **monotonic watermark** — commit the contiguous-completed offset | object-store-safe (Databricks-native) |
| Delta *(Step 2)* | table version / CDF | **monotonic watermark** — commit the version | object-store-safe (Databricks-native) |

The dispose flavor is the reason v1's design was awkward for a Databricks user (rename isn't atomic on
cloud object storage); the watermark flavor fixes it and is how the same engine runs as a Databricks
**Job** against Kafka/Delta. On Databricks the low-latency serving of results is DBR-proprietary
(Auto Loader / DLT / online tables) and stays glue-only; the engine + seam are portable.

## Correctness
- **At-least-once, never-drop:** a chunk whose processing throws is **not** committed → the source
  reclaims it on restart (dispose) or it is re-read from the last committed cursor (watermark). Re-adding
  a resolved record is a fast optimized no-op.
- **Concurrency-safe sinks:** each chunk writes its error/good frames as their own atomically-renamed
  single files (`de-*.parquet` / `af-*.parquet`) — never `Append` (which races the commit protocol
  across concurrent jobs). Row schema is identical to `ParquetStreamFeeder`'s, so `DeadLetterReprocess`
  and downstream dedup are unchanged.
- **Per-chunk staging:** `AddCore.run` writes `staging/<bounds>` (a shared path would clobber under
  concurrency); cleaned after commit.
- **Memory-bounded:** ≤ K chunks × `filesPerChunk` records in flight; references dropped per chunk.

## Job args (`glue.ParquetParallelFeeder`)
| Arg | Meaning | Default |
|---|---|---|
| `source` | `inbox` (kafka/delta are Step 2) | `inbox` |
| `inbox` / `processing` | inbox dir / in-flight claim dir (source=inbox) | — |
| `archive` | disposed shards moved here; empty ⇒ deleted | `""` |
| `recordsPerBatch` | records per batch; `1000` ⇒ **one partition/batch** (independent commit, straggler = 1 slot) | `1000` |
| `maxUnprocessedBatches` | worker threads = batches in flight; set **≥ `spark.cores.max`** so a straggler costs 1 of K | `200` |
| `recordsPerShard` | drainer shard size, so the inbox adapter maps records→files (source=inbox) | `1000` |
| `staging` | base for per-batch `AddCore` staging (`staging/<bounds>`) | `staging` |
| `deadLetter` / `output` | DLQ dir / affected-entity change-feed dir (empty ⇒ skip) | `""` |
| `trigger` | `default` (long-running) or `availableNow` (drain then exit) | `default` |
| `emptyMs` | idle window before `availableNow` exits | `30000` |
| `runId` | ties affected-entity rows to a run | `run` |

**Operating point — 1 partition per batch, K ≈ slot count (Rust-consumer emulation).** With
`recordsPerBatch=1000` each batch is a single task that commits on its own, so a huge-entity
straggler holds exactly **one** worker/slot and the other K-1 workers keep cycling fresh batches —
**no mid-stream tail**, the only idle is genuine end-of-input. Set `maxUnprocessedBatches` ≥
`spark.cores.max` (a straggler then costs 1 of K). This is the closest Spark gets to the Rust
consumer's per-thread pull. ⛔ The stall to avoid: FEW workers (K≈10) or MULTI-partition batches make
a worker wait for a batch's slowest partition, so a few stragglers park all workers and the cluster
starves (observed on `.142`). Records are the source-agnostic unit (Kafka/Delta use offsets/versions,
not files). ⚠ Validate throughput in a controlled/dedicated-DB arm — the shared live DB confounds it.

> Measured on `.142` (2026-08-07): with all feeder designs the engine slots stay **full** (`active:12/12`
> per executor) but ~80% are `sqlExecuting` — this arm is **DB-round-trip-bound**, so throughput and
> host CPU are set by the DB, not by feeder tuning. The overlapping engine's job is to guarantee the
> slots never idle on a straggler tail; it cannot raise a DB-bound ceiling.

## Measuring the win (see [`PERFORMANCE.md`](PERFORMANCE.md))
Both feeders are DB-bound, so `.142` CPU idle is only a weak proxy. Judge on: engine-thread duty cycle
(should stay high, no periodic tail dips), a deliberately-slow shard NOT idling other slots (asserted in
`ParquetParallelFeederSpec`), and DB-side resolved-row deltas. MQ ack rate reflects the drainer's persist
rate, not ER throughput — do not judge on it.

## Step 2
`KafkaSource` (offset watermark, `minPartitions` fanning one unpartitioned topic into N tasks) and
`DeltaSource` (version/CDF watermark) implementing the same seam, plus a throttled RabbitMQ→Kafka bridge
(cap the Spark consumer's lag). Same engine, same `AddCore` — only the source differs.
