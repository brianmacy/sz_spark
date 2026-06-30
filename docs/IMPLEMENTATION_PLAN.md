# sz_spark — Implementation Plan

**Build status (2026-06-30):** all 17 milestones implemented. `sbt test` = **77 tests green, 20 suites**
(unit + concurrency + spark-local on a real `local[*]` session). M9 build tasks are implemented and the
build loads; the full multi-GB `stageNatives`+`assembly` is the documented local gate (not run in CI).
**Real-engine verification (gaps closed):** `EngineIT` runs against a real engine — **4/4** on
PostgreSQL (`docker run postgres:16`, concurrent `local[4]` load → 6 records → 3 entities, 0 errors;
plus search and delete-of-absent no-op) and on SQLite via `./scripts/it-local.sh`. The **FAT jar
self-extracts and resolves entities on a slim `temurin:21-jre` container with no system Senzing**
(`SelfCheck`), proving dlopen-by-soname. `FatJarIT` + `verifyAssembly` confirm the **265 MB** jar
(libSz.so is already stripped). Delete-of-absent finalized as a benign no-op. Fixed an `isFatJar()`
dev-mode false-positive. Verified throughout against the installed `sz-sdk.jar` via `javap`.

**Status: stabilized.** Derived from the ratified `docs/DESIGN.md` (Spark 4.0 / Scala 2.13) by a
multi-agent planning pass, then reviewed by two independent critics (ordering/completeness and
Senzing-correctness, the latter verified against the Senzing-MCP). All **6 fatal findings** were folded
in: M8/M13 dependency corrections, M14/M15 test-gate additions, the `SecondEnvBuildGuard` wiring, and
the `setDefaultConfig` Java-signature fix. Re-confirm every Senzing signature against the installed
`sz-sdk.jar` at implementation time (open risks carried from `DESIGN.md` §9).

Execute milestones in `buildOrder`. Each milestone has a **test gate** that must be green before the
next dependent milestone starts. Pure/unit-testable code lands first; the highest-risk native/engine/
redo concerns are de-risked early as thin spikes; `SZ_IT=1` integration tests come last.

## Global prerequisites

- A **local licensed** Senzing dist at build time: installed `$SENZING_DIR`
  (`/opt/senzing/er/sdk/java/sz-sdk.jar`, `/opt/senzing/er/lib`, `/opt/senzing/data`,
  `/opt/senzing/er/resources`) or `.deb`s under `$SENZING_DEB_DIR/<arch>/` (`dpkg-deb -x`). **Never**
  commit/publish the SDK jar, native libs, `.deb`s, or FAT jar.
- **JDK 17** (Spark 4.0 baseline), **sbt 1.x**; pin `scalaVersion` to the **2.13.x** matching the exact
  target Databricks runtime (mismatch under `provided` scope = runtime `NoSuchMethodError`).
- `patchelf` and `readelf` on the build host (native staging + closure assertions).
- A co-located SQL DB for integration: **PostgreSQL** primary (SQLite dev-smoke, single-process only).
- `SENZING_ENGINE_CONFIGURATION_JSON` provided as env/secret at runtime (never hardcoded / never
  `setupEnv`); `PGSSLMODE=require` for cloud Postgres.
- **Senzing-MCP** + `sz-spark-faq` remain the source of truth — consult before each Senzing-touching
  milestone. JSON attribute mappings per data source via Senzing-MCP `mapping_workflow`.
- **GLIBC floor 2.34** (verified); confirm against the actual target runtime and publish as a binding
  precondition.

## Open decisions (defaults chosen; override anytime)

1. **Runtime pin:** Spark 4.0 / Scala **2.13.x**. *M0 hard-blocks on confirming the exact patch with the
   cluster owner before any code lands* — a wrong guess forces a recompile of everything.
2. **Primary DB:** **PostgreSQL** for the default build/CI integration (MySQL/MSSQL as documented
   branches; the bundled DB plugin defaults to `libpostgresqlplugin` → `libpq.so.5`).
3. **RedoJob durability:** default = modest per-run batches + convergent redo + scheduled re-drain (no
   persist-before-process); strict persist-before-process is a documented option.
4. **`SzConfigManager` API:** pinned now (see M10) — Java `setDefaultConfig(configJson)` (one arg) or
   `registerConfig(configJson, comment)` + `setDefaultConfigId(configId)`; re-confirm `replaceDefaultConfigId`
   against the installed jar.

## Build order

`M0 → M1 → M2 → M3 → M4 → M5 → M6 → M7 → M8 → M9 → M10 → M11 → M12 → M13 → M14 → M15 → M16`

Parallelizable once their deps are met: M1/M2 after M0; M3/M4 after M1; **M8 after M0** (pure, no engine
dep); M9 after M2. The critical path runs through the engine spike (M5) and Spark plumbing (M11).

---

## Milestones

### M0 — Project scaffold  ·  deps: none
Compilable empty sbt module with correct scopes, formatting, package skeleton, and a green CI lane.
- **Files:** `build.sbt`, `project/build.properties`, `project/plugins.sbt`, `.scalafmt.conf`,
  `src/main/scala/com/senzing/spark/package.scala`, `.github/workflows/ci.yml`, `README.md` (`.gitignore`
  already present).
- **Steps:** (1) `build.sbt` with Spark `% "provided"`, the SDK jar as an **unmanaged local** dep (no
  Senzing Maven coordinate), Jackson + PostgreSQL JDBC managed deps; (2) `project/` plugins
  (sbt-assembly, sbt-scalafmt) + `.scalafmt.conf`; (3) package tree `nativelib/ engine/ work/ jobs/
  model/`; (4) ensure `.gitignore` covers `src/main/resources/native/` + `**/.debs/`; (5) green
  baseline `scalafmtCheckAll`/`compile`/`test`; (6) CI runs those three and **never** runs
  `stageNatives`/`assembly`/`SZ_IT`.
- **⛔ Hard block:** confirm the target Databricks runtime → exact `scalaVersion` (2.13.x) with the
  cluster owner *before* writing code.
- **Test gate:** `sbt compile` ✓ · `sbt scalafmtCheckAll` ✓ · `sbt test` exit 0 (no specs) · CI compile
  lane green, with assembly/stageNatives/SZ_IT excluded.
- **Exit:** clean project compiles/formats, green CI lane; package skeleton matches `DESIGN.md` §1.

### M1 — Pure model + Schemas  ·  deps: M0
Immutable row case classes + Spark `StructType`s; the tagged-union staging contract jobs depend on.
- **Files:** `model/Rows.scala`, `model/Schemas.scala`, `test/.../model/SchemasSpec.scala`.
- **Steps:** rows per `DESIGN.md` §1 (`AffectedEntityRow{op:ADD|DELETE|REDO}`, `SearchResultRow`,
  `ErrorRow`); the tagged-union staging schema (good|error in one write, §5 option B); StructTypes
  field-for-field with the case classes; `SchemasSpec` covers mapping + encode/decode.
- **Test gate:** `SchemasSpec` ✓ (pure, no Spark session).
- **Exit:** all row types/schemas compile; tagged-union contract fixed.

### M2 — SPIKE: native layer + dlopen-by-soname proof  ·  deps: M0  ·  **highest risk, early**
`NativeBootstrap`, `NativeLibLoader`, `GlibcCheck`, `EngineSettings` with injectable seams.
- **Files:** `nativelib/{NativeBootstrap,NativeLibLoader,GlibcCheck,EngineSettings,NativePaths}.scala`
  + the four matching specs.
- **Steps:** (1) `NativeLibLoader` with an **injectable load fn** encoding the exact ordered chain
  `libgcc_s → libszzstd → libszvec → <db-plugin> → libSz`; (2) `NativeBootstrap` extraction (file-lock
  + `.ready` sentinel + atomic rename + subtree preservation), source/target injectable; (3)
  `GlibcCheck` + `EngineSettings` (three **distinct** PIPELINE dirs; fail-loud on unset) with injectable
  inputs; (4) specs against **synthetic** fixtures (no real Senzing binaries); (5) code comments state
  the **live** dlopen proof is deferred to FatJarIT (M16) and requires launch-time
  `spark.executorEnv.LD_LIBRARY_PATH=<extract>/lib` (no post-JVM-start substitute).
- **Test gate:** `NativeBootstrapSpec` (race/atomicity/subtree/floor/tmpfs) · `NativeLibLoaderSpec`
  (order + idempotency) · `GlibcCheckSpec` · `EngineSettingsSpec` (distinct dirs, fail-loud, CONFIGPATH
  correctness) — all ✓.
- **Exit:** four nativelib components with green unit specs; dlopen-by-soname load order + launch
  `LD_LIBRARY_PATH` requirement codified.

### M3 — Pure work utilities I: ErrorTaxonomy, Backoff, CircuitBreaker  ·  deps: M0, M1
- **Files:** `work/{ErrorTaxonomy,Backoff,CircuitBreaker}.scala` + specs.
- **Steps:** (1) re-confirm the Senzing exception hierarchy via `javap` + Senzing-MCP before coding
  match arms; (2) `ErrorTaxonomy` as an ordered match, **subclass-before-supertype**, `NOT_FOUND` a
  distinct category whose benign-skip semantics are decided later by DeleteIT; (3) `Backoff`/
  `CircuitBreaker` as pure injectable-clock/RNG state machines.
- **Test gate:** `ErrorTaxonomySpec` ✓ using **real** exception instances in the verified order ·
  `BackoffSpec` ✓ (deterministic + exhaustion) · `CircuitBreakerSpec` ✓ (trip/reset/fast-fail).
- **Exit:** classification + retry/breaker policy proven against real exception types.

### M4 — Pure work utilities II: InfoParser, Counters, ProgressLogger  ·  deps: M0, M1
- **Files:** `work/{InfoParser,Counters,ProgressLogger}.scala` + `InfoParserSpec`, `ProgressLoggerSpec`.
- **Steps:** (1) confirm `AFFECTED_ENTITIES` / search JSON schema via Senzing-MCP `response_schemas`;
  (2) `InfoParser` (Jackson, tolerant of empty AFFECTED_ENTITIES); (3) `Counters`/`ProgressLogger` with
  injected clock, **interval and cumulative** rates computed separately.
- **Test gate:** `InfoParserSpec` ✓ (empty/missing/malformed) · `ProgressLoggerSpec` ✓ (rates, prefix,
  final summary, **no** `getStats` coupling).
- **Exit:** full pure `work/` layer green with no engine/Spark dependency.

### M5 — SPIKE: engine single-active-instance lifecycle  ·  deps: M2  ·  **high risk**
`SzEngineProvider` create-once / destroy-at-JVM-shutdown + concurrency proof. **Introduces the shared
`SzEnvGuard`** (the second-env build guard) here, since this is the first place an env is built.
- **Files:** `engine/SzEngineProvider.scala`, `engine/SzEnvGuard.scala`,
  `test/.../engine/SzEngineProviderSpec.scala`, `test/.../engine/SzEnvGuardSpec.scala`.
- **Steps:** (1) make `SzCoreEnvironment.build()` swappable behind a builder fn for a recording fake;
  (2) double-checked single build, retained env, read/write lock, **liveness counter only** (never
  destroy at zero), **one ordered** shutdown hook (`destroy()` awaited → native cleanup); (3)
  `maybeEmitStats` time-cadence guard as the only `getStats` caller; (4) `SzEnvGuard` (process-wide
  AtomicBoolean) tripped by `SzEngineProvider` — **and reused by InitJob in M10** so neither builds a
  second env in one JVM; (5) re-confirm `newBuilder()/getEngine()/destroy()/getStats` signatures.
- **Test gate:** `SzEngineProviderSpec` ✓ (built-once, destroyed-once, **ordered destroy-before-cleanup**,
  release-to-zero never destroys, `getStats` throttled) · `SzEnvGuardSpec` ✓ (second build trips loudly).
- **Exit:** singleton lifecycle + concurrency contract proven with a fake; real build path wired to the
  native layer, queued for integration proof.

### M6 — ConfigDrift detection + reinit  ·  deps: M5
- **Files:** `engine/ConfigDrift.scala`, `ConfigDriftSpec`.
- **Steps:** (1) confirm `getActiveConfigId`/`getConfigManager().getDefaultConfigId()`/`reinitialize`
  are on **`SzEnvironment`** (not `SzEngine`) against the installed jar; (2) throttled `maybeReinit`
  (CAS timestamp) + write-lock reinit + `forceCheckAndReinit`.
- **Test gate:** `ConfigDriftSpec` ✓ (throttle window, reinit-on-difference, write-lock quiesce, force
  bypass, correct API target on a fake env).
- **Exit:** drift detection/reinit proven; ready for the worker.

### M7 — RecordWorker (pure per-partition verb driver)  ·  deps: M3, M4, M6, M1
- **Files:** `work/RecordWorker.scala`, `RecordWorkerSpec`.
- **Steps:** (1) pure function over an **injected verb fn** + ConfigDrift handle + `work/` utilities,
  emitting tagged-union rows; (2) exact taxonomy→action mapping (`DESIGN.md` §6), `NOT_FOUND`
  benign-skip behind a flag defaulting to route-as-bad-input until DeleteIT decides; (3) balanced
  `acquire`/`release` via `try/finally`; `LONG_RECORD` logs but **never cancels**.
- **Test gate:** `RecordWorkerSpec` ✓ for every taxonomy branch, retry-once, `RETRY_EXHAUSTED`
  no-backoff, breaker fast-fail, `LONG_RECORD` non-abort, dedup, balanced acquire/release, progress.
- **Exit:** engine-agnostic worker loop fully unit-tested.

### M8 — RedoJob source dequeuer (pure spec)  ·  deps: **M0**  ·  early redo de-risk
> **Fix applied:** dependency corrected to **M0 only** — `RedoSource` is pure/engine-free and does not
> need the engine spike (M5).
- **Files:** `jobs/RedoSource.scala`, `RedoSourceSpec`.
- **Steps:** (1) `RedoSource` as a pure dequeuer over an **injected `getRedoRecord` supplier** (no
  Spark, no real engine); (2) encode the batch limit and the **never-loop-on-`countRedoRecords`** rule;
  (3) defer the parallel job to M13 and the real proof to RedoIT (M16).
- **Test gate:** `RedoSourceSpec` ✓ (pulls until null/limit, stops at limit, empty→empty, no
  `countRedoRecords` loop).
- **Exit:** single-dequeuer source contract proven pure.

### M9 — FAT-jar build: stageNatives + overlay + patchelf + assembly + verify  ·  deps: M2
- **Files:** `project/{StageNatives,Overlay,Patchelf,VerifyAssembly}.scala`, `build.sbt`,
  `config/overrides/{data,resources,config}/.gitkeep`.
- **Steps:** (1) `StageNatives` (`dpkg-deb -x` or `$SENZING_DIR` copy) staging the **four** trees;
  derive the native closure via `readelf`; confirm the CONFIGPATH file set lives under
  `er/resources/templates/`; (2) `Overlay` (empty no-op), `Patchelf` (**siblings only, no
  `--force-rpath`, never `libSz`**), `VerifyAssembly` (exact-set assertion); (3) merge strategy +
  big-heap assembly in `build.sbt`; (4) commit **empty** `config/overrides/...` with `.gitkeep`, keep
  `native/` gitignored.
- **Test gate (local, not CI):** `sbt stageNatives` populates four trees · `sbt -J-Xmx8g assembly` ✓ ·
  verify task asserts the exact `readelf`-derived closure · `git status` clean of `native/` + `.debs/`.
  CI **excludes** stageNatives/assembly (no licensed dist; no-redistribution).
- **Exit:** FAT jar builds locally with the correct closure; live dlopen proof queued for FatJarIT.

### M10 — InitJob: SchemaApplier + config registration under DB lock  ·  deps: M2, M5, M1
Standalone run-once JVM main; **never** in an executor.
- **Files:** `jobs/{SchemaApplier,InitJob}.scala` + `SchemaApplierSpec`, `InitConfigLockSpec`
  (`SzEnvGuard` is reused from M5 — no new guard here).
- **Steps:** (1) confirm `getConfigManager()/getDefaultConfigId()/setDefaultConfig/registerConfig/
  setDefaultConfigId/replaceDefaultConfigId` + create-from-template via Senzing-MCP
  `generate_scaffold(initialize/configure)` and the installed jar — **use Java `setDefaultConfig(configJson)`
  (one arg) or `registerConfig(configJson, comment)` + `setDefaultConfigId(configId)`; the two-arg
  `setDefaultConfig` is C#/JS only**; (2) `SchemaApplier` (JDBC, idempotency probe `sys_cfg`, dialect
  branch); (3) `InitJob` with a DB-level lock around read-default-then-register (no TOCTOU), exactly one
  env, destroy+exit; (4) **reuse `SzEnvGuard` (M5)** so `InitJob` trips it too.
- **Test gate:** `SchemaApplierSpec` ✓ (dialect/idempotency/DDL+commit) · `InitConfigLockSpec` ✓
  (first-init via `setDefaultConfig`/`registerConfig`, **not** the C#/JS two-arg form; non-zero → no-op
  or `replaceDefaultConfigId`; no TOCTOU under a real lock primitive) · `SzEnvGuardSpec` covers both
  `SzEngineProvider` and `InitJob` callers.
- **Exit:** idempotent schema apply + locked config registration with exactly one env, unit-proven.

### M11 — Spark plumbing: SparkJob trait + SparkRecordOps  ·  deps: M7, M1
- **Files:** `jobs/{SparkJob,SparkRecordOps}.scala`, `SparkRecordOpsSpec`.
- **Steps:** (1) `SparkJob` bootstrap/arg-parsing + **speculation disabled** + random-repartition helper;
  (2) `SparkRecordOps` single-pass `mapPartitions` → single committed tagged-union write → read-back
  split (option B), committer keyed by **partitionId only**; (3) keep all Senzing types behind the
  injected verb so this layer is engine-free in tests.
- **Test gate:** `SparkRecordOpsSpec` ✓ on `local[*]` with a fake engine: **exactly-once** engine stage,
  random repartition, single-write+split, committer atomicity, dedup, speculation off, no per-task
  `getStats`.
- **Exit:** shared single-pass/two-sink pipeline proven; data jobs become thin wirings.

### M12 — Data jobs: AddUpdateJob, DeleteJob, SearchJob  ·  deps: M11, M5, M7
- **Files:** `jobs/{AddUpdateJob,DeleteJob,SearchJob}.scala`, `DataJobsSpec`.
- **Steps:** (1) confirm `SzRecordKey.of`, `addRecord/deleteRecord/searchByAttributes` signatures +
  overloads + the `SZ_WITH_INFO`/search flag enum via Senzing-MCP + jar; (2) wire each main onto
  `SparkRecordOps` with its verb + output mapping, reusing the worker (no parallel impls); (3) keep the
  `NOT_FOUND` benign-skip flag defaulting to bad-input pending DeleteIT.
- **Test gate:** `DataJobsSpec` ✓ on local Spark (fake engine): correct output/error rows, op tagging,
  dedup for mutating jobs, search pairs.
- **Exit:** three data jobs produce the designed DataFrames; only real-engine integration remains.

### M13 — RedoJob: scheduled parallel processor  ·  deps: **M8, M11**
> **Fix applied:** dependency corrected to **M8 + M11** (dropped the false dependency on M12 — RedoJob
> reuses `SparkRecordOps`/the worker, not the three data-job mains).
- **Files:** `jobs/RedoJob.scala`, `RedoJobSpec`.
- **Steps:** (1) confirm `processRedoRecord(redo, flags)`/`getRedoRecord()`/`countRedoRecords()`
  signatures; (2) wire `RedoJob`: `RedoSource` batch → DataFrame → random repartition → `SparkRecordOps`
  with the `processRedoRecord` verb, `op=REDO`; (3) **do not** entity-dedup the redo feed upstream
  (continuous-feed rule); (4) scheduled/recurring invocation (queue refills).
- **Test gate:** `RedoJobSpec` ✓ on local Spark (fake engine): dequeue→DataFrame, parallel
  `processRedoRecord`, `op=REDO` rows, **no `countRedoRecords` loop**, worker reuse.
- **Exit:** RedoJob implements the §7 scheduled-parallel pattern; real behavior queued for RedoIT.

### M14 — Concurrency + spark-local consolidation (integration gate)  ·  deps: M7, M11, M12, M13, M6, M10
- **Files:** `.github/workflows/ci.yml` (the suite already exists).
- **Steps:** (1) `sbt test` whole non-integration suite, fix cross-milestone drift (category vocab,
  schema mismatches); (2) deterministic `local[*]` sessions with clean teardown; (3) CI excludes
  SZ_IT-gated specs and never builds/publishes the FAT jar; (4) tag as the integration gate.
- **Test gate:** entire unit + concurrency + spark-local suite ✓ · `scalafmtCheckAll` ✓ · **`SzEngineProviderSpec`
  re-runs clean against the fully-wired codebase** (regression guard after M6/M7/M10–M13 wiring) · CI
  green with `SZ_IT` unset, assembly not invoked.
- **Exit:** 100% non-integration tests pass and CI-enforced; ready for SZ_IT.

### M15 — Integration tests (SZ_IT=1) against real Senzing + PostgreSQL  ·  deps: M14, M10
- **Files:** `src/it/.../{InitIT,LoadIT,DeleteIT,SearchIT,RedoIT,ReplicationConsistencyIT}.scala`,
  `project/ITConfig.scala`.
- **Steps:** (1) sbt `IntegrationTest` (`src/it`) gated by `SZ_IT=1` (skip when unset); (2) real local
  Senzing + PostgreSQL with `SENZING_ENGINE_CONFIGURATION_JSON`; (3) run `InitIT` then the rest; (4) use
  DeleteIT's **observed** delete-of-absent fact to finalize the `NOT_FOUND` branch + commit a regression
  test; (5) validate `InfoParser` against real WITH_INFO output.
- **Test gate (SZ_IT=1):** `InitIT` · `LoadIT` (persists across JVM restart) · `DeleteIT` · `SearchIT` ·
  `RedoIT` · **`ReplicationConsistencyIT`** (consumers re-query `getEntity` per affected id,
  last-write-wins convergence) — all ✓; `NOT_FOUND` branch finalized with a committed regression test.
- **Exit:** real engine/DB behavior proven; delete-of-absent + WITH_INFO open risks closed by fact.

### M16 — FAT-jar integration: FatJarIT + BundleClosureIT  ·  deps: M9, M2, M15  ·  **closes top risk**
- **Files:** `src/it/.../{FatJarIT,BundleClosureIT}.scala`, `docs/RUNBOOK.md`, `docs/DATABRICKS.md`.
- **Steps:** (1) build the FAT jar (M9) against a licensed dist; deploy to a supported **slim image with
  no system Senzing**; (2) `FatJarIT` with launch-time
  `spark.executorEnv.LD_LIBRARY_PATH=<extract>/lib` + ordered `System.load` → `build()` + a real verb
  succeed (closes dlopen-by-soname risk); (3) `BundleClosureIT` asserts the `readelf` closure + DB-client
  contract on the target image; (4) if the launch env var can't be set on the target cluster, record the
  deploy as **failing** (no post-JVM-start substitute) and surface in the runbook; (5) write
  `RUNBOOK.md` + `DATABRICKS.md` (GLIBC floor, `LD_LIBRARY_PATH`, speculation-off, scheduled RedoJob,
  secrets, runtime pin, no-redistribution).
- **Test gate (SZ_IT=1, supported image):** `FatJarIT` ✓ (dlopen-by-soname proven) · `BundleClosureIT` ✓
  (closure + DB-client contract) · runbook/Databricks docs complete.
- **Exit:** dlopen-by-soname + DB-client-image risks closed by integration proof; deployment documented;
  the example is complete and deployable locally.

---

## Carried open risks (from `DESIGN.md` §9, to close during execution)

Runtime pin confirmation (M0); `reinitialize()` concurrency certification (confirm with Senzing);
dlopen-by-soname proof on the supported image (M16); GLIBC floor vs actual runtime (M16); DB-client
image contract (M16); delete-of-absent behavior (M15 DeleteIT); JSON attribute mapping per data source
(Senzing-MCP `mapping_workflow`); exact V4 signatures incl. `SzConfigManager` family re-confirmed
against the installed `sz-sdk.jar` at implementation time.
