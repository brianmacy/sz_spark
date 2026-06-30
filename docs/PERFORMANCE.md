# Performance & sizing: scaling Senzing on Spark/Databricks

How to size and tune a Senzing-on-Spark deployment for high throughput, including the path to
**billions of records** on **Databricks**. Senzing-on-Spark performance follows the **standard Senzing
SDK performance model** — this document maps that model onto Spark's executor/core knobs and the
**co-located SQL database**.

> **No observed benchmark numbers yet.** This project has not been run at cluster scale, so this
> document gives the **tuning model, the knobs, and Senzing's published guidance** — not measured
> throughput for this codebase. The "Benchmarking" section below is the plan to produce real numbers;
> until it runs, do not quote a records/sec figure for sz_spark. Concrete numbers depend on data,
> config, hardware, and database, as Senzing's own guidance stresses.

## The performance model (standard Senzing)

Senzing is **"share nothing but the database"**: it scales by running more engine threads/processes
until **the database or network** becomes the bottleneck. Key facts from Senzing's general performance
guidance:

- **One engine per process, shared across real OS threads.** Senzing creates a per-thread database
  context automatically. (Green threads / fibers / goroutines do **not** scale here — you need real OS
  threads. In this project, the "process" is the executor JVM and the "threads" are the Spark task
  threads driving the shared engine.)
- **The database is the primary bottleneck**, after data mapping. Senzing runs a largely
  **auto-commit OLTP** workload, so **commit latency dominates** — by 10×+ relative to most other
  factors.
- **Data contention** is the main scaling exception: many records resolving the **same** entity
  simultaneously serialize on engine locks. This is exactly why this project **partitions input
  randomly and never groups by a resolution key** (see below).

Sources: Senzing `performance-general`, `postgresql-performance`, and the Senzing docs "Database
Tuning" (verify specifics against the Senzing-MCP, which is authoritative).

## The one knob: executor cores

Concurrency is **executor/core count** — there is no threads-per-worker setting. Total Senzing
concurrency is the sum of `spark.executor.cores` across all executors. To increase throughput, add
executors/cores **until the database becomes the limiter**, then scale the database (see below). Beyond
that point, adding executors only increases contention and connection pressure.

## Co-located database — the highest-leverage decision

Because commit latency dominates, **keep the database close to the executors**:

- Place executors and the database in the **same region / availability zone / VPC / subnet**.
  Cross-AZ round-trips (1–2 ms each) are individually tiny but compound across the millions/billions of
  commits in a large load, measurably cutting throughput.
- The workload is **IO-bound**, not CPU-bound — network and disk latency to the DB matter more than
  executor CPU.

## Database tuning (PostgreSQL)

Senzing's auto-commit OLTP workload benefits enormously from disabling per-commit disk flushes. From
Senzing's published PostgreSQL guidance (confirm current values via the Senzing-MCP):

```
synchronous_commit = off        # by far the biggest lever for the auto-commit workload
enable_seqscan     = off
wal_writer_delay   = 1000
```

- **Aurora PostgreSQL** additionally uses (per Senzing's Aurora parameter-group guidance)
  `autovacuum_max_workers = 5` and, when the pglogical extension is present (common in Aurora
  replication setups), `pglogical.synchronous_commit = 0`.
- **Replication caveat:** synchronous replication re-introduces the commit-flush cost. On AWS Aurora,
  a read replica in a **different AZ forces synchronous commit back on**. Customers often run the
  initial historical load **without** replication and enable it afterward, when steady-state DB load is
  much lower.
- **Aurora Serverless v2** shows significantly degraded performance vs. v1 for this workload.
- MSSQL / MySQL have their own equivalents — see Senzing's `mssql-performance` /
  database-tuning guidance.

### Validate DB latency before blaming the engine

Senzing provides a single-connection insert benchmark — `check_repository_performance()` (V4, on
`SzDiagnostic`; the `checkDBPerf` command in the CLI tools). It measures how many auto-commit inserts complete in a few
seconds; **the faster, the more scalable the system**. Senzing's guidance cites targets on the order of
thousands of inserts in a 3-second window from one connection on a well-configured cloud DB; well-tuned
systems see sub-millisecond per-insert latency. Run this first — if single-connection insert latency is
poor, no amount of Spark parallelism will help. Also sanity-check raw network latency with
`psql … \timing` + `select 1;` (no Senzing schema involved).

## Connection planning

**Each Senzing engine thread holds one database connection for its lifetime.** Size the database's
`max_connections` for the total across the cluster, plus the init/admin/redo tasks:

```
max_connections  ≥  Σ(executor.cores across all executors)  +  (init + redo + admin/monitoring)  +  headroom
```

Under-provisioned `max_connections` causes connection-pool exhaustion under load — see
[`TROUBLESHOOTING.md`](TROUBLESHOOTING.md).

## Executor memory (native / off-heap)

The engine's working set is **off-heap** and must be reserved as `spark.executor.memoryOverhead`, not
`spark.executor.memory`:

```
spark.executor.cores          = N
spark.executor.memory         = 2g          # JVM heap — modest (records stream)
spark.executor.memoryOverhead = (4 + N) g   # ≈ 4 GB base engine + 1 GB per concurrent Sz thread
```

Memory scales with the **same** cores knob as concurrency. If per-node native RAM is tight, **prefer
more executors with moderate cores over a few fat executors**. → FAQ `deployment/executor-memory-sizing`.

## Partitioning (throughput-critical)

- **Partition input randomly** (round-robin `repartition(N)` or a random salt); tune `N` to total
  executor cores.
- **Never** sort or repartition by name / address / zip / any resolution key. Grouping records that
  resolve to the same entity causes lock contention and a **2–10× throughput loss**. →
  FAQ `deployment/database-and-input-partitioning`.

## Redo cadence

Redo processing competes with loaders for the same engine and database. For high-throughput loads,
either run `RedoJob` on a **schedule** that interleaves with loading, or drain redo as a distinct phase
after a batch load (`load → drain → read`). Run a **single** `RedoJob` instance at a time. Keep per-run
redo batch sizes modest — `getRedoRecord()` removes the record into the DataFrame, so a whole-job crash
after dequeue can lose the in-flight batch (redo is convergent and re-drains, but small batches limit
exposure). → FAQ `redo/redo-strategy`.

## Monitoring throughput

- **Per-task progress lines** report interval + cumulative records/sec — the primary live throughput
  signal (in-memory counters; no engine round-trip).
- **`engine.getStats()`** is process-global and **reset-on-read**; this project calls it at most once
  per JVM on a time cadence. Its `retries` / contention fields indicate engine-level lock contention.
- **`countRedoRecords()`** is a coarse health gauge for queue depth — **never** a loop condition (full
  table scan).
- Watch `LONG_RECORD` warnings: large-entity merges run long, uncancellable JNI verbs.

## Scaling to billions of records

The path is the standard Senzing scale-out, expressed in Spark terms:

1. **Tune the database first** (`synchronous_commit=off`, IO-optimized storage, co-location). Validate
   with `check_repository_performance()` — the DB is the ceiling.
2. **Size `max_connections`** for the full cluster's cores plus redo/init/admin.
3. **Add executors/cores** to raise concurrency, with `memoryOverhead = (4 + cores) GB` per executor.
4. **Partition randomly**; never by a resolution key.
5. **Scale executors until the database saturates**, then scale the database (instance size, IO,
   read/commit topology) — not more executors.
6. **Drain redo** on a schedule so results stay complete; redo volume grows with load.
7. For the initial historical load specifically, consider **deferring replication** until steady state.

> The actual records/sec and cluster shape for a given target (e.g. 10B records on Databricks) must be
> established by the benchmark below against your data, config, and database — they are not assumed.

## Benchmarking (to produce real numbers)

Not yet run (NEXT_STEPS #11). The intended methodology:

- Representative dataset (≥ 1M records to start; scale up), real co-located PostgreSQL.
- Sweep executor/core counts; record **records/sec (interval + cumulative)**, **redo queue depth**, DB
  CPU / IO / connection count, and `engine.getStats()` contention fields.
- Establish the point where the **database** becomes the limiter.
- Capture results here and in [`DESIGN.md`](DESIGN.md). Until then, no sz_spark throughput number is
  published.
