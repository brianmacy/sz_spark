# Project Status

**Date:** 2026-06-30
**Branch:** main
**Commits:** 0 — the entire working tree is intentionally uncommitted, pending user review before the first commit.

## What is complete

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

The working tree has no commits. Global project rule: **ask the user before committing**. Nothing is lost — git tracks all files and the user may review the full diff before approving the first commit.
