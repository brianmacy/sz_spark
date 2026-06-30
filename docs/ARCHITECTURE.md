# Architecture: how Senzing runs on Spark

A reader-facing overview of how this project runs the **Senzing V4 entity-resolution SDK** inside
**Spark / Databricks** jobs against a **co-located SQL database**. For the implementation-level design
(module layout, exact signatures, open risks) see [`DESIGN.md`](DESIGN.md); for the binding operational
rules see [`RUNBOOK.md`](RUNBOOK.md).

## The deployment pattern

Senzing's largest commercial customers run the engine **embedded in the compute tier**, talking to a
**co-located SQL datastore** (PostgreSQL / MSSQL / MySQL). This project reproduces that pattern on
Spark: the engine is a native C library loaded into each executor JVM, and the database is the shared
state. Senzing's architecture is **"share nothing but the database"** — executors do not coordinate
with each other; they all read and write the same repository.

## One engine per executor JVM (not per task)

The single most important rule. Senzing's guidance is **one engine per process, shared across all
threads**; this project applies that per Spark **executor JVM**:

- The `SzEnvironment` / `SzEngine` is built **exactly once per executor JVM** and destroyed **only at
  JVM shutdown** — never per task, partition, or record, and never destroyed-then-rebuilt inside a
  living JVM (Databricks reuses executor JVMs across stages).
- `acquire()` / `release()` maintain a **liveness counter only** — they never build or destroy the
  engine.
- A single ordered JVM shutdown hook runs `env.destroy()` **before** native-dir cleanup.

→ FAQ `architecture/threading-and-engine-lifecycle`.

## Single-threaded per task; scale by executor count

Each Spark task drives the shared engine **single-threaded** (every verb runs under a read lock). There
is **no "threads per worker" knob** — **concurrency equals the number of executor cores across the
cluster**. To go faster, add executors/cores; the engine's own threads scale with the same knob.

```
total Senzing concurrency  =  Σ (spark.executor.cores) across all executors
```

This is deliberately simple: there is one tuning dimension (executor/core count), and it governs both
parallelism and the off-heap memory you must reserve (see [`PERFORMANCE.md`](PERFORMANCE.md)).

## Native, off-heap memory

The engine is C, so its working set is **off-heap**. It must be reserved as
`spark.executor.memoryOverhead`, **not** `spark.executor.memory` (heap). Rule of thumb: **≈ 4 GB base
engine + 1 GB per executor core**. Under-sizing `memoryOverhead` gets executors OOM-killed under load.
The JVM heap stays modest because records stream. → FAQ `deployment/executor-memory-sizing`.

## The operations and their output DataFrames

Four operations, each its own `spark-submit` application from the **same FAT jar**:

| Op | Job | Verb | Output |
|---|---|---|---|
| Add / update | `AddUpdateJob` | `addRecord(..., SZ_WITH_INFO)` | deduped **affected entity IDs** + error rows |
| Delete | `DeleteJob` | `deleteRecord(..., SZ_WITH_INFO)` | deduped affected entity IDs + error rows |
| Search | `SearchJob` | `searchByAttributes(...)` | **request paired with results** + error rows |
| Redo | `RedoJob` | `processRedoRecord(..., SZ_WITH_INFO)` | affected entity IDs (`op=REDO`) + error rows |

`AFFECTED_ENTITIES` comes from parsing the `SZ_WITH_INFO` response — **not** a separate query. It is a
change-**notification** feed of entity IDs (no content, no order). Consumers build a settled view by
**re-querying** `getEntity` / `getEntityByRecordId` per affected ID with last-write-wins.
→ FAQ `architecture/output-dataframes`.

## Every record produces a good row or an error row

A per-record error taxonomy routes failures instead of crashing the task:

- `SzBadInputException` → bad row: route to the error DataFrame, **continue**.
- `SzRetryableException` → backoff-retry, then route if exhausted.
- Config-relevant (`SzUnknownDataSource`, `SzConfiguration`, …) → drift-check + retry once.
- Other `SzException` / `Error` → **fail the task loudly**.

→ FAQ `architecture/error-handling`. The verbs are synchronous, **uncancellable** JNI calls, so a slow
record is logged (`LONG_RECORD`) but never aborted. → FAQ `troubleshooting/slow-records`.

## Single-pass execution, two sinks

Engine calls are **side effects that must run exactly once**. Writing one lazy DataFrame to two sinks
(output + errors) would re-execute the lineage and re-call the engine. So each task calls the engine
once per record, emits a **tagged-union row** (good | error) to a committer-backed staging table in a
single action, then reads it back and splits into output/errors with **zero re-execution**.
`spark.speculation` is **hard-disabled** for the loader/delete jobs.

Engine mutations are therefore **at-least-once** (the committer makes only the output *files*
exactly-once). A re-run re-applies `addRecord` (an idempotent replace, so entity state converges) and
regenerates redo, which `RedoJob` absorbs.

## Initialization is a separate, run-once step

`InitJob` is a standalone JVM step, run **once** before any Spark data job — **never** on an executor.
Two `SzEnvironment` instances in one process corrupt the C library and crash. `InitJob` applies the
schema DDL (PostgreSQL/MySQL/MSSQL; SQLite auto-creates) and registers the default config, then exits.
The data jobs assume both already exist. → FAQ `architecture/initialization-separation`.

## Redo is a scheduled, parallel job

When a record changes entity state, Senzing defers cascading re-evaluations to a **global redo queue**.
If redo is not drained, results are incomplete. `RedoJob` uses `getRedoRecord()` as a **source**: a
single driver-side dequeuer pulls redo records into a DataFrame, which is repartitioned and processed
in parallel like any other op. It is **recurring** — `getRedoRecord() == null` is **not** "done" (the
queue refills, and redo begets redo). **Never** use `countRedoRecords()` as a loop condition (it is a
full table scan; monitoring only). → FAQ `redo/redo-strategy` and [`DESIGN.md`](DESIGN.md) §7.

## Do not partition by a resolution key

Partition input **randomly** (round-robin / random salt). **Never** sort or repartition by name,
address, zip, or any resolution key: grouped records that resolve to the same entity collide on engine
locks and throughput collapses (2–10× loss). → FAQ `deployment/database-and-input-partitioning`.

## End-to-end flow

```
InitJob (run once)  ──►  schema DDL + default config in the co-located DB
                                  │
        ┌─────────────────────────┼─────────────────────────┐
        ▼                         ▼                          ▼
   AddUpdateJob / DeleteJob    SearchJob                  RedoJob (scheduled)
   random-partitioned          read-only                 getRedoRecord() source
   addRecord/deleteRecord      searchByAttributes         processRedoRecord
        │                         │                          │
        ▼                         ▼                          ▼
   affected-entity + error    request+result + error     affected-entity (op=REDO) + error
   DataFrames                 DataFrames                  DataFrames
        └───────────────► all executors share one co-located SQL repository ◄───────────┘
```

See [`EXAMPLES.md`](EXAMPLES.md) for runnable `spark-submit` invocations of each job.
