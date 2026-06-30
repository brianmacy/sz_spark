# Changelog

All notable changes to this project will be documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

> Note: as of 2026-06-30 there are **zero commits** in this repository. The entire implementation
> below constitutes the first commit (pending user approval). The `[Unreleased]` section describes
> all work done in the initial build session (M0–M16).

### Added

**Project scaffold (M0–M2)**
- `build.sbt` with Scala 2.13.16 + Spark 4.0.2 (provided), Jackson 2.18.2 (bundled, aligned to Spark 4.0), PostgreSQL JDBC 42.7.7, ScalaTest 3.2.19, Mockito 5.18.0; sbt 1.12.13.
- `project/plugins.sbt` with sbt-assembly 2.3.1 and sbt-scalafmt 2.6.1; scalafmt 3.9.6.
- `.scalafmt.conf` aligned to Spark/standard conventions.
- `CLAUDE.md` capturing architecture, SDK guardrails, build/test/run commands, hard constraints.
- `.gitignore` (JVM/sbt template; native staging dirs and FAT jar excluded).
- CI (`.github/workflows/ci.yml`) running scalafmt + the full 78-test plumbing suite; third-party actions SHA-pinned to latest majors (checkout v7.0.0, setup-java v5.4.0, cache v6.1.0). `.github/dependabot.yml` (github-actions, 21-day cooldown). All Scala/sbt deps at latest stable (sbt kept on 1.x; 2.0 is a breaking major).

**Data model and engine plumbing (M3–M4)**
- `model/Rows` — `AffectedEntityRow`, `SearchResultRow`, `ErrorRow`, and the `StagingRow` tagged union; `model/Schemas` derives the Spark `StructType`s.
- `engine/SzEngineProvider` — create-once/destroy-at-JVM-shutdown singleton with ordered shutdown hook + RW lock.
- `engine/SzEnvGuard` — one-active-environment-per-process enforcement (throws on double build).
- `engine/EngineLifecycle` — acquire/release liveness counter (does NOT destroy at zero; see FAQ).
- `nativelib/EngineSettings` — read `SENZING_ENGINE_CONFIGURATION_JSON` + rewrite the three PIPELINE paths to the native extract dir.

**Config drift / live config updates (M5)**
- `engine/ConfigDrift` — periodic (~60 s) and error-triggered reinit; double-checked locking under write lock prevents reinit stacking; CAS throttle prevents error-storm stampede.

**Record processing (M6–M7)**
- `work/Verbs` — `SZ_WITH_INFO` flag wrappers for add/update/delete; search.
- `work/InfoParser` — extract `AFFECTED_ENTITIES` from WITH_INFO JSON; parse search results.
- `work/ErrorTaxonomy` — classify `SzException` subtypes: bad input → error DataFrame; retryable → backoff; config-related → drift-check-then-retry; systemic → task fail.
- `work/Backoff` — jittered exponential backoff with configurable budget.
- `work/CircuitBreaker` — open on consecutive systemic errors, fail task loudly.
- `work/RecordWorker` — drives the engine single-threaded per task; emits good/error rows; calls ConfigDrift check.
- `work/ProgressLogger` — per-task counters (succeeded/skipped/errored/retried/long); periodic interval + cumulative rate; labeled prefix + final summary.

**Spark job layer (M8)**
- `jobs/SparkJob` — base trait (SparkSession + config).
- `jobs/SparkRecordOps` — single-pass `flatMap` with two-sink output (good DataFrame + error DataFrame).
- `jobs/AddUpdateJob`, `DeleteJob`, `SearchJob` — concrete Spark jobs.
- `jobs/RedoJob` — standalone redo drain (loop `getRedoRecord` + `processRedoRecord`).
- `jobs/RedoSource` — lazy iterator over the redo queue (terminates on null/empty; batch-limited).
- `jobs/InitJob` — one-time admin job: schema DDL + default-config registration (separate from Spark executor path).
- `jobs/SchemaApplier` — apply DDL by dialect (PostgreSQL; stubs for MSSQL/MySQL; none for SQLite dev).

**FAT jar / native self-extraction (M9, M2/M5)**
- `project/NativeStaging.scala` — `stageNatives` + `verifyAssembly` sbt tasks: copy lib/data/resources + the CONFIGPATH file set from `$SENZING_DIR` into `src/main/resources/native/linux-<arch>/`, overlay `config/overrides/`, `strip` bundled siblings, and `patchelf --set-rpath '$ORIGIN'` on bundled siblings (never `libSz.so`).
- `nativelib/NativeBootstrap` — fat-jar detection (marker resource AND jar-file code source); SHA-256 jar hash; extract once per node under `sz-spark-<sha>/` (file-lock + `.ready` sentinel, atomic temp→rename); `extractJarResources`; stale-dir cleanup.
- `nativelib/NativeLibLoader` — ordered `System.load` of the native siblings (libgcc_s → libszzstd → libszvec → db-plugin → libSz) so dlopen-by-soname resolves; paired with the launch `LD_LIBRARY_PATH`.
- `nativelib/GlibcCheck` — detect host glibc; enforce the 2.34 floor; fail loudly if below.
- `config/overrides/{data,resources,config}/` — empty overlay dirs shipped as a DevOps hook (no-op when empty); documented in the FAQ.

**Diagnostics (M12)**
- `diag/SelfCheck` — standalone smoke test: build env, add a record, retrieve it, delete it, destroy.
- `diag/DeleteProbe` — probe delete behavior on absent records (verifies `SzNotFoundException` handling).
- `diag/ShowOutput` — run a small add/search batch and print the output DataFrames.

**CI / supply chain (M13)**
- `.github/workflows/ci.yml` — push/PR gate: `scalafmtCheckAll` + `sbt test` (78 unit tests; integration tests excluded).
- `.github/dependabot.yml` — weekly `github-actions` updates; 21-day cooldown. (Dependabot has no sbt support, so Scala/sbt deps are bumped manually.)

**Integration tests (M14–M15)**
- `it/EngineIT` (5 tests, tagged `IntegrationTest`) — real PostgreSQL + SQLite: load, search, live-config-update, delete-of-absent record, redo drain.
- `it/FatJarIT` (1 test, tagged `IntegrationTest`) — Docker container (`temurin:21-jre`, no `/opt/senzing`) self-extracts FAT jar and resolves 6 records → 3 entities.
- `it/EngineSmoke` — lightweight smoke used by `it-local.sh`.
- `scripts/it-local.sh` — bash runner: provisions a fresh SQLite DB, runs `InitJob` (register config + TEST data source), then runs `EngineIT` against the real engine.

**Documentation (M16)**
- `docs/DESIGN.md` — architecture, threading model, config drift, error taxonomy, FAT jar build/runtime.
- `docs/IMPLEMENTATION_PLAN.md` — milestone breakdown M0–M16.
- `docs/RUNBOOK.md` — build, deploy, operate, troubleshoot.
- `docs/DATABRICKS.md` — Databricks-specific cluster config, init scripts, job submission.
- `README.md` — project overview and quick-start.
- `.claude/faqs/` — 14 FAQ entries across 7 categories (architecture, build, deployment, redo, sdk, testing, troubleshooting).
- `STATUS.md`, `NEXT_STEPS.md`, `CHANGELOG.md` (this file).

**Post-M16 additions (same session, pre-first-commit)**
- `docs/tutorials/spark-onprem.md`, `docs/tutorials/aws-emr.md`, `docs/tutorials/databricks.md` — three DRAFT end-to-end deployment tutorials; not yet validated on live clusters.
- `.claude/faqs/deployment/tutorials.md` — FAQ entry covering tutorial scope, DRAFT status, and what needs cluster validation before promoting to non-draft.
- `.claude/faqs/deployment/database-and-input-partitioning.md`, `executor-memory-sizing.md`, `redistribution.md` — three new deployment FAQ entries.
- `engine/ConfigDrift` reinforced: double-checked locking prevents reinit stacking; a new `ConfigDriftSpec` unit test covers the no-double-reinit invariant.
- `it/EngineIT` extended: live-config-update test (activates a new config ID mid-run) and delete-of-absent-record test added; `scripts/it-local.sh` verified with `CONFIGPATH=/etc/opt/senzing` (non-default path, exercises the self-extraction rewrite logic).
- CI actions SHA-pinned: `.github/workflows/ci.yml` updated from bare `@v4` tags to full commit hashes (`checkout@9c091bb # v7.0.0`, `setup-java@1bcf9fb # v5.4.0`, `cache@55cc834 # v6.1.0`); no bare version tags remain.
- All Scala/sbt deps bumped to latest stable as of 2026-06-30 and re-verified with `sbt test` (78 unit tests, all green).
