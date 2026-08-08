# Changelog

All notable changes to this project will be documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added
- **Entity-mart replication — Phase 1 (`mart/` package)** — the customer's real-time Databricks
  export. Driven by the affected-entity feed (`WITH_INFO` → `AFFECTED_ENTITIES`) the feeders already
  emit → per-id `getEntity` → MERGE into Delta. Design of record:
  `sz_spark_entity_map_delta_replication.md`. Components:
  - **`EntityMartSchema`** — the four read-optimized Delta projections (`entity`, `entity_record` =
    the entity map, `relationship` normalized `lo<hi`, `entity_doc`) + `_sync_state`/`_quarantine`, as
    `CREATE TABLE` DDL that works for both a path-based OSS-Delta table (local proxy) and a Unity
    Catalog name (Databricks). CDF + deletion vectors + liquid clustering on the MERGE key;
    `refresh_seq`/`updated_at` bookkeeping so a delayed/replayed batch is a no-op.
  - **`GetCore`** — the engine bracket (one `SzEnvironment` per executor JVM, mirrors `SparkRecordOps`;
    staged write/read-back so the reads fire once); `getEntity` under an **explicit §7.4 flag OR**
    (never a `*_DEFAULT_FLAGS` composite); `SzNotFoundException` → GONE (tombstone), `Systemic` →
    rethrow-loud, else → `_quarantine`.
  - **`EntityMartRows`** — pure, Jackson-parse transform (field paths verified against the Senzing-MCP
    v4 entity-read schema) → the four frames + tombstones + a canonical `entity_hash` change-gate;
    7-case golden-file unit test (`EntityMartRowsSpec`), incl. hash stability-under-reorder.
  - **`EntityMartSink`**/`LocalDeltaSink` — SQL-through-locator so a `DatabricksUcSink` slots in by
    name; monotonic (`refresh_seq`-guarded) MERGEs + tombstone cascade. The **`relationship` MERGE is
    column-wise `coalesce`** so a single-endpoint refresh never nulls the opposite direction's
    `match_key`/`rev_match_key`; the tombstone cascade uses **`MERGE ... WHEN MATCHED THEN DELETE`**
    (OSS delta-spark rejects `IN (subquery)` in `DELETE`). Also (Phase 1.1, aligning to the Senzing
    data-mart Entity Refresh Pattern): a **hash change-gate** (`selectChanged` drops entities whose
    stored `entity_hash` is unchanged — "skip if unchanged" — so a re-resolution no-op writes nothing)
    and **orphan-record reconcile** (a record DELETED from a surviving entity has its stale
    `entity_record` row removed; a MOVE is re-keyed by the gaining entity's refresh). All proven by
    `EntityMartSinkIT` (5 cases, a tagged local Spark+Delta IT — no engine — that also confirms the
    `CLUSTER BY`+DV+CDF DDL runs on OSS delta-spark 4.0.0). The canonical `entity_hash` now uses
    non-printable field/record separators (US/RS) to remove a boundary-collision risk.
  - **`EntityMartSync`** — the glue driver: read the affected feed → dedup ids → GetCore → **change-gate**
    → rows → sink; advance the `_sync_state` watermark; `trigger=availableNow|loop` + `cadenceMs` cadence.
- **RabbitMQ→Kafka bridge** (`glue.MqToKafka`, Step 2b) — a plain-JVM competing consumer (the
  RabbitMQ→Kafka analog of the `MqToParquet` drainer) that moves records from the queue onto the topic
  `KafkaSource` reads, **throttled so the Spark consumer stays ≤ `maxLag` (default 5,000,000) records
  behind**: `lag = latestKafkaOffset − committedOffset` (the committed offset read from the SAME
  checkpoint the feeder writes), pausing drains while `lag ≥ maxLag` so unread Kafka stays bounded —
  Kafka retention + this cap replace unbounded queue growth. Same write-ahead invariant as the parquet
  drainer: **produce-THEN-ack** (crash between ⇒ RabbitMQ redelivers ⇒ duplicate ⇒ idempotent
  `add_record` absorbs it). Unit-tested (`MqToKafkaSpec`): produce-before-ack ordering + the throttle
  boundary, Mockito channel/producer. Args: `amqpUrl` / `queue` / `bootstrapServers` / `topic` /
  `checkpoint` / `maxLag` / `batchRecords`.
- **Delta source for the parallel-batch feeder** (`glue.DeltaSource`, Step 2c) — the watermark seam over
  a Delta table's **Change Data Feed**; cursor = table **version**, reusing `glue.OffsetWatermark`.
  `nextChunk` reads CDF for a bounded window of versions `[cursor, min(cursor+versionsPerBatch, latest+1))`
  filtered to new rows; `commit` advances the contiguous-prefix watermark; `reclaim` is a no-op. Prereqs:
  CDF enabled + a STRING `value` column holding the JSON body. ⚠ **version-granular, not
  row-count-granular** (a large commit is one batch — prefer Kafka for the tightest tail-freeness).
  `delta-spark` added `Provided`. `source=delta` (args `tablePath` / `checkpoint` / `startingVersion` /
  `versionsPerBatch`). Unit-tested (`DeltaSourceSpec`): the version-window arithmetic.
- **`glue.KafkaSourceIT`** — broker end-to-end IntegrationTest (produces to a fresh topic, drives
  `KafkaSource` + `OffsetWatermark`, asserts count-bounded ranges + projection + watermark advance).
  Tagged `IntegrationTest` (excluded from `sbt test`); run with `SZ_IT=1 SZ_KAFKA_BOOTSTRAP=<broker>`.
- `kafka-clients` (3.9.1, matching Spark 4.0.1) added **bundled** so the standalone bridge runs from the
  FAT jar and the driver-side `endOffsets` call needs no `--packages`.
- **Kafka source for the parallel-batch feeder** (`glue.KafkaSource`, Step 2) — the watermark-flavor
  `RecordSource` counterpart to the on-prem `InboxSource`, object-store-safe / Databricks-native. Same
  `OverlappingBatchEngine`, only the source differs (`source=kafka`). Design (locked): **ONE
  unpartitioned topic** — read parallelism is `minPartitions` fanning the single partition into N tasks,
  never Kafka partitions, so records are not grouped by a resolution key. `nextChunk` claims a
  **count-bounded** `[cursor, min(cursor+recordsPerBatch, latest))` range (a large lag becomes many small
  1-partition batches, not one straggler-prone giant one); `commit` advances the new `glue.OffsetWatermark`
  over the **contiguous-completed prefix** — required because the engine's K workers commit out of order —
  persisting a double-buffered checkpoint (`offset-<topic>-0` + `.bak`); `reclaim` is a no-op (restart
  re-reads the committed offset, replay = cheap no-op re-add). The `spark-sql-kafka-0-10` connector is
  `Provided` (supply at launch via `--packages`). Args: `bootstrapServers` / `topic` / `checkpoint` /
  `startingOffset` (cold-start only) / `minPartitions`. Unit-tested: `OffsetWatermarkSpec` (out-of-order,
  gap, idempotency, durability, restart — real local FS) + `KafkaSourceSpec` (count-bounding, bounds
  round-trip); broker end-to-end is an `IntegrationTest`. Remaining Step 2: a throttled RabbitMQ→Kafka
  bridge (≤ ~5M-record lag cap) and a `DeltaSource`. See [`docs/PARALLEL_BATCH_FEEDER.md`](docs/PARALLEL_BATCH_FEEDER.md) §Step 2.
- **Source-agnostic overlapping-batch feeder** (`glue.ParquetParallelFeeder`) — the tail-killing
  alternative to `glue.ParquetStreamFeeder`. Structured Streaming commits micro-batches strictly
  sequentially, so every batch is exposed to a straggler idling the whole cluster (measured on `.142`:
  76% idle mid-tail). The new engine keeps **K chunk-jobs in flight** under `spark.scheduler.mode=FAIR`
  so a freed slot pulls the next partition from *any* in-flight chunk — a slow record holds exactly one
  slot, never the cluster. New pieces: `glue.RecordSource` (the source seam: `nextChunk`/`commit`/
  `reclaim` with two commit flavors — **dispose** for transactionally-ack'd MQ, **monotonic watermark**
  for object-store-safe Kafka/Delta), `glue.InboxSource` (the RabbitMQ-via-drainer dispose adapter),
  `glue.OverlappingBatchEngine` (K persistent worker threads, each claim→process→commit→loop), and
  `glue.ShardIo` (atomic single-file sinks + shard claim/dispose/reclaim, now shared with `MqToParquet`).
  **Operating point: `recordsPerBatch=1000` ⇒ one partition per batch, `maxUnprocessedBatches` ≥
  `spark.cores.max`** — each batch commits independently so a huge-entity straggler parks 1 of K workers
  and the rest keep cycling: no mid-stream tail, only genuine end-of-input. At-least-once / never-drop
  (uncommitted chunks are reclaimed on restart); same `AddCore` and same dead-letter contract as the
  streaming feeder. Record-based knobs only (`recordsPerBatch` / `maxUnprocessedBatches`) — no exposed
  partition/concurrency counts. See [`docs/PARALLEL_BATCH_FEEDER.md`](docs/PARALLEL_BATCH_FEEDER.md).
  Kafka/Delta watermark adapters + a throttled RabbitMQ→Kafka bridge are Step 2 (same seam).
- **Dead-letter capture in the streaming feeder** (`glue.ParquetStreamFeeder`): each micro-batch's
  `SplitResult` is now persisted instead of discarded — the `errors` frame to a durable `deadLetter`
  dir (the on-prem DLQ) and the `good` frame to an append-only `output` change-feed, both
  `SaveMode.Append` and both opt-in (empty path ⇒ no write, back-compat). At-least-once with
  downstream dedup on `(dataSource, recordId, category)` / `(dataSource, recordId, entityId)`; no
  shard is ever dropped. Closes the silent-orphan gap (a record with a `DSRC_RECORD` row but no
  resolved-entity row and no error artifact). See [`docs/DEAD_LETTER.md`](docs/DEAD_LETTER.md).
- **`glue.DeadLetterReprocess`** — a thin one-pass job that reads the dead-letter dir, keeps the
  reprocessable categories (`RETRY_EXHAUSTED`, `CONFIG_RELEVANT`, `REPLACE_CONFLICT`), leaves terminal
  ones (`BAD_INPUT`, `NOT_FOUND`) quarantined, and re-emits the survivors as `InputRecord` parquet
  shards into a re-feed dir. Cadence is left to a cron/supervisor.
- **`diag.StatsPlugin` / `diag.StatsSampler`** — an opt-in Spark plugin
  (`--conf spark.plugins=com.senzing.spark.diag.StatsPlugin`) that starts exactly one sampler thread
  per executor JVM and emits the engine's reset-on-read `getStats()` on a time cadence
  (`spark.senzing.statsIntervalMs`, default 5 min) to the driver log under the `SZ_STATS` prefix
  (executor-log fallback if the driver send fails). Zero cost / zero code path when not listed; never
  forces an engine build on a non-Senzing executor. Adds `SzEngineProvider.tryEngine()` /
  `EngineLifecycle.peek()` non-building probes.

### Changed
- **`DATA_SOURCE` + `RECORD_ID` are read from the record body and REQUIRED.** The loaders/drainer
  previously stamped `DATA_SOURCE` from an external launch arg; both keys now come from the record
  JSON (as `RECORD_ID` already did), and a record missing either is routed to the `BadInput`
  dead-letter without an engine call — never silently stamped. The `dataSource` launch arg is gone.
  (Search/redo paths, which need no record key, are unaffected.)
- **Removed the per-batch `repartition(N)`** from `ParquetStreamFeeder.foreachBatch`. It inserted a
  shuffle stage (executor slots idle on I/O) and could reduce the partition count below the input file
  count; measured ~0.1% of batch wall time and closed no throughput gap. The file source's partitions
  now feed the executor slots directly (read + `add_record` fuse into one pipelined stage). See
  [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md) §"Measured findings".

### Docs
- **`docs/tutorial/` — a progressive, adopter-facing Guides series** (new): `README` (index +
  RabbitMQ-vs-Kafka decision guide), `01-architecture` (engine-per-JVM + the two seams), `02-getting-started`,
  `03-rabbitmq-setup`, `04-kafka-setup`, `05-adopt-your-own-source` (implement `RecordSource`), and
  `06-adapt-your-own-replication` (the affected-entity feed → your entity store). Diagram-first, concise,
  self-contained; folds in the hard-won lessons the reference docs scattered (engine-build parity, the
  ack-on-persist backpressure requirement, `shardRecords`=batch-size, the 1-partition/straggler-free
  operating point, the contiguous-prefix watermark). Linked as the lead entry from `README.md`.
- **`docs/BUILD_AGAINST_FLEET_ENGINE.md`** (new) + `.claude/faqs/build/engine-build-parity.md` (new):
  the mandatory engine-build-parity check. `sbt stageNatives` bakes whatever engine `SENZING_DIR`
  holds into the FAT jar, silently; a stale `/opt/senzing` (`4.4.0.26151`) vs the fleet's
  `4.4.0.DEVELOPMENT` was the dominant term in a ~20% apparent feeder deficit on 2026-08-07. Documents
  the `apiVersion` (`get_stats`) parity check, the hybrid `SENZING_DIR` rebuild (DEV natives + matching
  `sz-sdk.jar`, and the `UnsatisfiedLinkError` from mismatched jar/natives), and why the native-drift
  gate cannot catch this.
- `docs/PERFORMANCE.md`: finding #2 **corrected** — the apparent Spark deficit DID reproduce and its
  root cause was the stale bundled engine (above), not the earlier confound hypothesis; rebuilding
  against the fleet engine restored CPU/throughput parity. Methodology note gains "confirm same engine
  build (and libpq major) FIRST." The rest of "Measured findings" (loader-source equivalence at matched
  build, repartition-removal rationale, duty-cycle/invariant-counter methodology, libpq-17 chunked-rows)
  stands.
- `docs/PARALLEL_BATCH_FEEDER.md` (new): design of the source-agnostic overlapping-batch feeder — the
  straggler-tail rationale, the partition-level work-stealing mechanism, the `RecordSource` seam and its
  two commit flavors, correctness (at-least-once / concurrency-safe sinks), job args, the 1-partition
  operating point, and the engine-build-parity caveat for A/B measurement.
- New `docs/DEAD_LETTER.md`; `docs/RABBITMQ_INGEST.md` reconciled to the final feeder (no per-batch
  repartition, dead-letter/output sinks); `docs/JOB_LAYERING.md` gains the `glue` reprocess job and a
  `diag` row; README reflects the three features and indexes the new docs.

## [0.1.2] - 2026-06-30

### Added
- Reader-facing, search-indexable docs under `docs/` (the crawler indexes README + everything under
  `docs/`): `ARCHITECTURE.md` (how Senzing runs on Spark), `BUILD.md` (FAT-jar packaging),
  `EXAMPLES.md` (runnable `spark-submit` for each job), `PERFORMANCE.md` (sizing + standard Senzing
  performance model — DB co-location, PostgreSQL tuning, connection planning; no fabricated numbers,
  benchmarks-pending), `TROUBLESHOOTING.md` (Spark-specific native-load / memory / DB failure modes).
- Deepened `DATABRICKS.md`: Volumes/DBFS jar location, cluster libraries, init scripts, secrets/Unity
  Catalog, notebook-vs-JAR, autoscaling caveats (marked not-yet-validated on a live cluster).
- README links the new docs.

### Fixed
- Cross-document consistency: reconciled the assembly heap flag to `-J-Xmx8g` (`-J-Xmx4g` minimum)
  across BUILD/DESIGN/RUNBOOK/tutorial (was a 4g/8g split); corrected `libSz.so` size to ~430 MB in
  DESIGN (was ~450 MB in one place); removed a duplicated Spark-conf block in DATABRICKS.
- Doc accuracy (caught by review against source): `EXAMPLES.md` `InitJob` now shows the required
  `db=<jdbcUrl>` argument — without it the schema DDL is silently skipped on Postgres/MySQL/MSSQL.

## [0.1.1] - 2026-06-30

### Fixed
- **CI now goes green on hosted `ubuntu-latest`.** Added an apt-install step that fetches
  `senzingsdk-runtime` from Senzing's public apt repo at build time (mirroring the official
  `senzing/senzingsdk-runtime` Dockerfile) into `/opt/senzing`, so CI compiles and runs the full
  78-test suite. Previously the `Verify Senzing SDK` step always failed — no licensed dist exists on
  hosted runners. No self-hosted runner, no committed SDK, no redistribution; the FAT jar is still
  never built or published in CI.
- CI: pass `SENZING_ACCEPT_EULA` (and `DEBIAN_FRONTEND=noninteractive`) through `sudo` on the apt
  install. The `senzingsdk-runtime` preinst reads that variable and otherwise drops to an interactive
  EULA `read </dev/tty` that hangs the runner; `sudo` resets the environment by default, so the
  job-level export never reached the preinst.
- CI: add `sbt/setup-sbt` (SHA-pinned, v1.4.0) — sbt is not pre-installed on `ubuntu-latest`.
- FAQ MCP server (`.claude/faq_server.py`): refresh the index synchronously before serving each
  request instead of in a background thread afterward, so a query issued immediately after editing or
  adding a FAQ no longer returns stale results.

## [0.1.0] - 2026-06-30

Initial Senzing-on-Spark reference implementation (M0–M16).

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
