# Project Status

**Date:** 2026-08-06
**Branch:** bem_rabbitmq_ingest
**State:** the M0–M16 reference implementation is in place; this branch adds the **streaming ingest
path** (parquet inbox → long-running feeder → engine) and, in the current uncommitted changelist,
three features on top of it. Ask the user before committing.

## Current branch work (uncommitted changelist)

New assets + docs for the streaming feeder, pending review/commit:

| Feature | Components |
|---|---|
| Dead-letter handling | `glue.ParquetStreamFeeder` now persists each batch's `SplitResult` — `errors` → durable `deadLetter` dir (DLQ), `good` → `output` change-feed, both `SaveMode.Append`, both opt-in; `glue.DeadLetterReprocess` replays the reprocessable categories. Tests in `ParquetStreamFeederSpec`. See `docs/DEAD_LETTER.md`. |
| `getStats` sampler | `diag.StatsPlugin` (Spark `SparkPlugin`: executor + driver) + `diag.StatsSampler` — one sampler thread per executor JVM, reset-on-read `getStats()` on a cadence → driver log (`SZ_STATS`). Opt-in via `spark.plugins`. Adds non-building probes `SzEngineProvider.tryEngine()` / `EngineLifecycle.peek()`. Tests in `diag/`. |
| Repartition removal | dropped the per-batch `repartition(N)` in `ParquetStreamFeeder.foreachBatch` (pure overhead, ~0.1% of wall, closed no gap). |

Measured findings captured in `docs/PERFORMANCE.md` §"Measured findings": the Rust consumer and Spark
feeder are performance-equivalent at the same duty cycle; on a shared, growing DB use duty cycle +
invariant per-record counters, not wall clock.

The prior streaming-feeder scaffolding on this branch (`core/` + `glue/` split, `AddCore`,
`ParquetStreamFeeder`, the RabbitMQ two-stage design) is already committed; see
`docs/RABBITMQ_INGEST.md` and `docs/JOB_LAYERING.md`.

## What is complete (M0–M16 baseline)

The full Senzing-on-Spark reference implementation was built end-to-end in this session (milestones M0–M16):

| Area | Components |
|---|---|
| Native self-extraction | `NativeBootstrap`, `NativeLibLoader`, `NativeStaging` (sbt task), patchelf `$ORIGIN` rpath patching, SHA-256-keyed extract dir, file-lock + `.ready` sentinel, shutdown cleanup |
| Engine singleton | `SzEngineProvider` (create-once/destroy-at-JVM-shutdown), `SzEnvGuard` (one-env-per-process enforcement), `EngineLifecycle` (acquire/release liveness counter) |
| Config drift | `ConfigDrift` — double-checked reinit under write lock, CAS throttle (~1/min), no reinit stacking; enables live config updates without job restart |
| Record processing | `RecordWorker`, `SparkRecordOps` (single-pass + two-sink), `ErrorTaxonomy`, `Backoff`, `CircuitBreaker`, `ProgressLogger`, `InfoParser` |
| Jobs | `AddUpdateJob`, `DeleteJob`, `SearchJob`, `RedoJob`, `InitJob` (separate one-time admin), `SchemaApplier` |
| Diagnostics | `SelfCheck`, `DeleteProbe`, `ShowOutput` |
| FAT jar | `stageNatives` sbt task, `patchelf` rpath rewrite, `sbt assembly` → 265 MB jar; `libSz.so` stripped before bundle |
| Tests (unit) | 78 tests, 20 suites — all green (`sbt test`). Excludes integration tests (tagged `IntegrationTest`). |
| Integration tests | `EngineIT` 5/5 on real PostgreSQL + SQLite via `./scripts/it-local.sh`; `FatJarIT` container self-extraction on `temurin:21-jre` with no `/opt/senzing` |
| Docs | `docs/DESIGN.md`, `docs/IMPLEMENTATION_PLAN.md`, `docs/RUNBOOK.md`, `docs/DATABRICKS.md`; `docs/tutorials/` — 3 DRAFT deployment tutorials (spark-onprem, aws-emr, databricks) |
| CI | `.github/workflows/ci.yml` — scalafmtCheckAll + sbt test on push/PR; third-party actions SHA-pinned to latest majors (checkout v7.0.0 `9c091bb`, setup-java v5.4.0 `1bcf9fb`, cache v6.1.0 `55cc834`) |
| Dependabot | `.github/dependabot.yml` — weekly, github-actions only (no sbt support), 21-day cooldown |
| FAQ MCP | `.claude/faqs/` — 7 categories, 18 entries; deployment category extended with `database-and-input-partitioning`, `executor-memory-sizing`, `redistribution`, and `tutorials` entries |
| Integration test config | `EngineIT` and `scripts/it-local.sh` verified with `CONFIGPATH=/etc/opt/senzing` (not the default `/opt/senzing/er/resources`); self-extraction rewrite tested |

## Known gaps (not blocking the first commit)

1. **True multi-JVM/multi-node cluster run** — all integration testing used `local[4]` or a single-node Docker Compose stack. Cluster-scale validation (multiple executor JVMs, Spark standalone or YARN/K8s) has not been done.
2. **MSSQL and MySQL dialects** — `SchemaApplier` branches by dialect but only PostgreSQL DDL is exercised; MSSQL/MySQL paths are untested.
3. **`reinitialize()` concurrency certification** — the MCP notes it does not explicitly certify `reinitialize()` is safe under concurrent verbs even with the read/write lock pattern. Needs Senzing confirmation before relying on it under heavy config churn.

## CI actions pin status

`.github/workflows/ci.yml` SHA-pins all three third-party actions to latest majors — no bare `@v` tags remain. Dependabot covers the `github-actions` ecosystem with a 21-day security cooldown.

## Uncommitted state

The current changelist (dead-letter + reprocess, `StatsPlugin` sampler, repartition removal, and the
accompanying docs) is **uncommitted**, pending review. Global project rule: **ask the user before
committing**. Nothing is lost — git tracks all files and the user may review the full diff before
approving the commit.
