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
| **KafkaSource** (built) | offset | **monotonic watermark** — [`OffsetWatermark`](../src/main/scala/com/senzing/spark/glue/OffsetWatermark.scala) commits the contiguous-completed offset; `reclaim` is a no-op (restart re-reads the committed offset) | object-store-safe (Databricks-native) |
| **DeltaSource** (built) | table version / CDF | **monotonic watermark** — same `OffsetWatermark` (version as cursor); `reclaim` is a no-op. ⚠ version-granular, not row-count-granular | object-store-safe (Databricks-native) |

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
| `source` | `inbox` or `kafka` | `inbox` |
| `inbox` / `processing` | inbox dir / in-flight claim dir (source=inbox) | — |
| `archive` | disposed shards moved here; empty ⇒ deleted | `""` |
| `bootstrapServers` / `topic` | Kafka brokers / topic (source=kafka) | — |
| `checkpoint` | durable committed-offset dir (source=kafka) | — |
| `startingOffset` | `earliest`\|`latest`\|`<number>` — **cold start only** (checkpoint governs after) | `earliest` |
| `minPartitions` | Kafka read fan-out of the one topic into N tasks (source=kafka) | `1` |
| `tablePath` | Delta table path (source=delta) | — |
| `startingVersion` | `latest`\|`<number>` — **cold start only** (source=delta) | `0` |
| `versionsPerBatch` | Delta versions per batch (source=delta) | `1` |
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

⚠ **Before any cross-loader A/B (feeder vs Rust consumer), verify engine-build parity first.** The FAT
jar bundles whatever engine `SENZING_DIR` held at build time; a stale one silently ships an old engine
and can be the *dominant* term in an apparent feeder deficit (this is exactly what a ~20% gap turned out
to be on 2026-08-07 — a `4.4.0.26151` jar vs the fleet's `4.4.0.DEVELOPMENT`; parity was restored by
rebuilding). Confirm the feeder's runtime `apiVersion` (`get_stats`) equals the fleet's — see
[`BUILD_AGAINST_FLEET_ENGINE.md`](BUILD_AGAINST_FLEET_ENGINE.md) and [`PERFORMANCE.md`](PERFORMANCE.md)
finding #2.

## Step 2 — Kafka source, RabbitMQ→Kafka bridge, and Delta source (all built)

**`KafkaSource` is built** ([`KafkaSource.scala`](../src/main/scala/com/senzing/spark/glue/KafkaSource.scala))
— the watermark-flavor seam over **ONE unpartitioned topic** (partition 0). Read parallelism comes from
`minPartitions` fanning that single partition into N tasks, **not** from Kafka partitions — so records
are never grouped by a resolution key (which would create the cross-key entity-lock contention the
project avoids). `nextChunk` claims `[cursor, min(cursor+recordsPerBatch, latest))` — a **count-bounded**
range, so a large lag becomes many small 1-partition batches, never one straggler-prone giant batch —
and `commit` advances [`OffsetWatermark`](../src/main/scala/com/senzing/spark/glue/OffsetWatermark.scala)
over the **contiguous-completed prefix**. Because the engine's K workers commit out of order, the
watermark holds a completed-but-behind-a-gap range until the gap fills, then sweeps forward and persists
(double-buffered `checkpoint/offset-<topic>-0` + `.bak`). A restart re-reads from the committed offset;
the replayed tail is a handful of cheap optimized no-op re-adds (at-least-once). `reclaim` is a no-op.

- **Launch:** `source=kafka bootstrapServers=… topic=… checkpoint=<durable dir> [startingOffset=earliest|latest|<n>] [minPartitions=1]`.
  The `spark-sql-kafka-0-10` connector is `Provided` — add it at submit with
  `--packages org.apache.spark:spark-sql-kafka-0-10_2.13:<sparkVersion>` (present on Databricks).
- **Correctness:** `OffsetWatermark`'s out-of-order/gap/idempotency/restart invariants are unit-tested
  (`OffsetWatermarkSpec`, real local FS); the count-bounding + bounds round-trip in `KafkaSourceSpec`.
  Broker end-to-end is an `IntegrationTest` (needs a live Kafka), like `EngineIT`.

**The RabbitMQ→Kafka bridge is built** ([`MqToKafka.scala`](../src/main/scala/com/senzing/spark/glue/MqToKafka.scala))
— a plain-JVM competing consumer (the RabbitMQ→Kafka analog of the [`MqToParquet`] drainer) that moves
records from the queue onto the topic, **throttled so the Spark consumer stays ≤ `maxLag` (default 5M)
records behind**: `lag = latestKafkaOffset − committedOffset` (the committed offset read from the SAME
checkpoint the feeder writes), and while `lag ≥ maxLag` it pauses draining — Kafka retention + this cap
replace unbounded queue growth. Same write-ahead invariant as the parquet drainer: **produce-THEN-ack**
(a crash between → RabbitMQ redelivers → duplicate Kafka record → idempotent `add_record` absorbs it).
Args: `amqpUrl` / `queue` / `bootstrapServers` / `topic` / `checkpoint` / `maxLag` / `batchRecords`.

**`DeltaSource` is built** ([`DeltaSource.scala`](../src/main/scala/com/senzing/spark/glue/DeltaSource.scala))
— the watermark seam over a Delta table's **Change Data Feed**; cursor = table **version**, same
`OffsetWatermark`. `nextChunk` reads CDF for a bounded window of versions `[cursor, min(cursor+versionsPerBatch,
latest+1))` filtered to new rows. Prereqs: CDF enabled (`delta.enableChangeDataFeed=true`) + a STRING
`value` column holding the JSON body. ⚠ **version-granular, not row-count-granular** — a single large
commit is one batch; keep source commits modest or raise `recordsPerBatch` so the engine repartitions a
big version. For the tightest tail-freeness prefer Kafka. `delta-spark` is `Provided` (present on
Databricks / add via `--packages io.delta:delta-spark_2.13:4.0.0`).

**Testing:** `MqToKafkaSpec` (produce-then-ack write-ahead + throttle boundary, Mockito channel/producer),
`DeltaSourceSpec` (version-window arithmetic), and `KafkaSourceIT` (broker end-to-end, `IntegrationTest` —
`SZ_IT=1 SZ_KAFKA_BOOTSTRAP=… `). Kafka/Delta/RabbitMQ end-to-end need live infra.
