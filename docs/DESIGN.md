# sz_spark — Implementation Design

**Status: PROPOSED (not yet ratified).** Produced by a multi-agent design pass that adversarially
stress-tested the design and **mechanically verified the Senzing facts against the installed dist**
(`readelf -d/-V` on `/opt/senzing/er/lib/*.so`, `javap` on `/opt/senzing/er/sdk/java/sz-sdk.jar`) plus
the Senzing-MCP. The adversarial pass did **not** reach zero open issues — the "Open risks" section is
the live list and must be closed (several only by integration tests against a real engine) before this
is treated as final. Re-confirm every Senzing signature against the **installed** `sz-sdk.jar` at
implementation time; the Senzing-MCP remains the authoritative source.

**Target runtime:** Spark 4.0 / Scala 2.13 (Spark 4 dropped Scala 2.12). Pin `scalaVersion` to the
exact target Databricks runtime — a 2.12↔2.13 mismatch under `provided` scope is a runtime
`NoSuchMethodError`.

---

## 1. Module layout

Single sbt module, root package `com.senzing.spark`; Senzing-touching code isolated from Spark glue.
A Scala `object` is one instance per executor JVM.

**`nativelib/`**
- **`NativeBootstrap`** — FAT-jar detect via marker resource `native/linux-<arch>/lib/libSz.so`;
  `os.arch` map (amd64→x86_64, aarch64→aarch64). `ensureExtracted()` extracts **once per node** into
  `$SENZING_EXTRACT_DIR/<sha256-of-jar>/`, guarded by `FileChannel.tryLock` + `.ready` sentinel;
  **atomic** extract (unpack to `<sha>.tmp`, fsync, rename); pre-extract free-space floor (default
  1 GB, fail loudly); refuse tmpfs unless `SENZING_ALLOW_TMPFS=true`; preserves subtree structure for
  `data/`, `resources/`, and the CONFIGPATH tree (lib/ stays flat for `$ORIGIN`). Returns
  `NativePaths(libDir, dataDir, resourcesDir, configDir)`. Registers **no** shutdown hook
  (`SzEngineProvider` owns the single ordered hook).
- **`NativeLibLoader`** — the dlopen fix (see §3). `loadNativeLibsInOrder(libDir)` `System.load()`s
  every non-system `.so` in dependency order before SDK init:
  `libgcc_s.so.1 → libszzstd.so → libszvec.so → <db-plugin>.so → libSz.so`.
- **`EngineSettings`** — reads `SENZING_ENGINE_CONFIGURATION_JSON` (fail loudly if unset; never
  hardcode / never `setupEnv`); sets `PGSSLMODE=require` if absent; in FAT-jar mode rewrites the three
  PIPELINE keys to **distinct** extract subdirs: `SUPPORTPATH=<dataDir>`, `RESOURCEPATH=<resourcesDir>`,
  `CONFIGPATH=<configDir>`.
- **`GlibcCheck`** — fails loudly before any native load if runtime glibc < the verified floor (2.34).

**`engine/`**
- **`SzEngineProvider`** — THE per-JVM singleton (only place besides `InitJob` that builds an env).
  `engine()` builds **exactly once** (double-checked, `AtomicBoolean builtOnce`):
  `GlibcCheck → NativeBootstrap.ensureExtracted → NativeLibLoader.loadNativeLibsInOrder →
  SzCoreEnvironment.newBuilder().settings(...).instanceName("sz-spark-exec").verboseLogging(false)
  .build() → env.getEngine()`. **Retains** the `SzEnvironment` (config-drift needs it). **One ordered
  JVM shutdown hook:** `env.destroy()` (awaited) THEN extract-dir cleanup. `withReadLock` wraps every
  verb; reinit takes the write lock. `acquire()`/`release()` maintain a **liveness counter only —
  never destroy at zero.** `maybeEmitStats()` is the sole caller of `engine.getStats()` (process-
  global, reset-on-read), at most once per JVM on a time cadence.
- **`ConfigDrift`** — `maybeReinit(env)` throttled ~60 s via CAS-guarded volatile timestamp; compares
  `env.getActiveConfigId()` vs `env.getConfigManager().getDefaultConfigId()` (**both on
  `SzEnvironment`, not `SzEngine`**); if different, acquires the write lock (drains in-flight verbs)
  and `env.reinitialize(defaultId)`. `forceCheckAndReinit(env)` bypasses the throttle for the
  config-relevant retry path.

**`work/`** (pure, no Spark): `RecordWorker` (per-partition, single-threaded verb driver + counters +
retry/backoff + LONG_RECORD timing + per-partition `CircuitBreaker` + bounded per-task affected-entity
dedup set); `ErrorTaxonomy`; `Backoff`; `CircuitBreaker`; `InfoParser` (AFFECTED_ENTITIES / search
results); `ProgressLogger` (interval + cumulative rate, in-memory counters only); `Counters`.

**`jobs/`**: `SparkRecordOps` (the one shared `mapPartitions` plumbing); `AddUpdateJob`, `DeleteJob`,
`SearchJob`, `RedoJob`, `InitJob` (each an `object` with `main`); `SparkJob` trait (arg parsing,
SparkSession bootstrap, committer-backed staging writers).

**`model/`**: `AffectedEntityRow(dataSource, recordId, entityId, op{ADD,DELETE,REDO}, runId)`;
`SearchResultRow(requestJson, resultJson)`; `ErrorRow(dataSource, recordId, payload, category,
errorCode, message, attempts)`; `Schemas`.

**Resources**: `src/main/resources/native/linux-{x86_64}/{lib,data,resources,config}/` (gitignored,
staged at build time). `config/overrides/{data,resources,config}/` committed but **empty** (DevOps
overlay hook).

## 2. One-time init (`InitJob`) — separate from the Spark job

A standalone JVM `main`, run **exactly once** per repository before any Spark data job, **never** in an
executor (two `SzEnvironment` instances in one process corrupt the C library and crash ~6 min in; and
the schema is not auto-created for PostgreSQL/MySQL/MSSQL).

1. `GlibcCheck → ensureExtracted → loadNativeLibsInOrder → EngineSettings` (same native libs/config the
   executors use); set `PGSSLMODE=require` for cloud Postgres.
2. **`SchemaApplier` (plain JDBC, no SDK loaded for this step)**, dialect-branched: PostgreSQL/MySQL/
   MSSQL → idempotency probe first (`SELECT EXISTS ... tablename='sys_cfg'`), skip if present, else
   apply `resources/schema/szcore-schema-{postgresql,mysql,mssql}-create.sql` and commit. SQLite
   (dev smoke only) → skip DDL; the SDK auto-creates the file during config registration.
3. **Config registration under a DB-level lock** (PostgreSQL advisory lock, or an init-lock row for
   MySQL/MSSQL): build exactly one `SzCoreEnvironment`; via `env.getConfigManager()` read
   `getDefaultConfigId()` **under the lock** (returns `long`, 0 when none): if 0 (greenfield) →
   create-from-template then `setDefaultConfig(configJson, comment)` (verified by `javap` on the
   installed jar: the Java SzConfigManager has BOTH `setDefaultConfig(String):long` and
   `setDefaultConfig(String,String):long` — both register-and-set in one call and return the new
   config id), or `registerConfig(configJson, comment)` + `setDefaultConfigId(configId)`; if non-zero →
   no-op, or `replaceDefaultConfigId(currentId, newId)` (also present in the Java jar) for an
   intentional change. Release lock. (Signatures confirmed against `/opt/senzing/.../sz-sdk.jar`.)
4. `env.destroy()`; exit 0.

Data jobs assume schema + default config exist. If a user override changes the **registered** config
(data sources/features/ER rules), the operator must re-run `InitJob`.

## 3. Engine lifecycle + native loading

**Create-once per executor JVM, destroyed only at JVM shutdown** — never per task/partition/record,
and **never destroyed-then-rebuilt within a living JVM.** (The SDK permits recreate-after-destroy, but
Databricks reuses executor JVMs across stages, so refcount-to-zero-then-rebuild churns build/destroy
needlessly; the single-active-instance constraint makes stage overlap fragile; teardown is costly.)
`acquire()`/`release()` are a liveness counter only. One ordered shutdown hook runs `env.destroy()`
(awaited) **before** native-dir cleanup — unordered hooks race and can SIGSEGV destroy against
unlinked `.so` files.

**Native loading (highest-risk area, grounded in `readelf` facts):**
- `libSz.so` NEEDED = `libssl.so.3, libcrypto.so.3, libm, libc, ld-linux`; RUNPATH=`$ORIGIN`.
- `libszvec.so` and `libszzstd.so` are **dlopen'd by soname** (not in libSz's NEEDED) and need
  `libgcc_s.so.1`. The DB plugin (`libpostgresqlplugin.so` → `libpq.so.5`; MSSQL → ODBC; MySQL →
  `libmysqlclient`) is also dlopen'd by soname.
- **`$ORIGIN`/RUNPATH does NOT apply to dlopen-by-soname** — the loader applies a lib's RUNPATH only to
  its direct NEEDED. So a single `System.load(libSz)` is insufficient. The fix is **both**:
  (a) set `spark.executorEnv.LD_LIBRARY_PATH=<extractDir>/lib` at executor **launch** (the only
  mechanism that works for dlopen-by-soname; never mutate post-JVM-start), and
  (b) `NativeLibLoader` `System.load()`s every sibling in dependency order before `libSz` so each
  later dlopen resolves an already-loaded handle.
- **`libstdc++` is NOT needed** for this dist (no GLIBCXX/CXXABI undefined). Bundle into `lib/` only
  the NEEDED non-system libs a slim image may lack: `libgcc_s.so.1`, and `libssl.so.3` +
  `libcrypto.so.3` if the image lacks OpenSSL 3.

### Executor memory (native / off-heap)

The engine is native C, so its working set is **off-heap** and must be reserved as
`spark.executor.memoryOverhead`, not `spark.executor.memory`. Sizing rule: **≈ 4 GB base engine +
1 GB per concurrent Sz thread** (= `spark.executor.cores`). Memory therefore scales with the same
executor/cores knob as concurrency; under-sizing `memoryOverhead` gets containers OOM-killed under
load. JVM heap stays modest (records stream). See RUNBOOK / DATABRICKS for the conf.

## 4. Build (FAT jar)

`sbt` single module + sbt-assembly + sbt-scalafmt. Spark deps `% "provided"`. The Senzing SDK jar is
an **unmanaged local dependency** (`unmanagedJars += <SENZING_DIR>/er/sdk/java/sz-sdk.jar`; filename is
`sz-sdk.jar`, **not** a Maven coordinate), plus Jackson and the DB JDBC driver for `InitJob`.

`stageNatives` (pre-assembly sbt task, output gitignored):
1. Source Senzing from a **local licensed copy**: `$SENZING_DEB_DIR/<arch>/*.deb` via `dpkg-deb -x`
   (avoids the EULA preinst), or an installed `$SENZING_DIR`.
2. Stage four trees into `src/main/resources/native/linux-<arch>/`: `lib/`, `data/`, `resources/`, and
   `config/` (the CONFIGPATH file set — `cfgVariant.json`, `customGn|On|Sn.txt`, `defaultGNRCP.config`
   — verified to live under `er/resources/templates/`, **not** `er/resources/` root). The three
   PIPELINE keys point at three **different** trees.
3. **Native closure** (derived mechanically, asserted by the verify task): bundle only the NEEDED
   non-system libs a slim image may lack (`libgcc_s.so.1`; `libssl.so.3`/`libcrypto.so.3` if absent).
4. **dlopen-by-soname resolution** (see §3): launch-time `LD_LIBRARY_PATH` + ordered `System.load`.
5. **DB-client image contract** (hard): the executor image must provide the chosen DB client's full
   closure on the system loader path. Optional air-gapped variant stages the entire `ldd`-derived
   closure into `lib/` and relies on the launch `LD_LIBRARY_PATH` (not `$ORIGIN`).
6. **Overlay** `config/overrides/{data,resources,config}/*` onto the staged stock trees (user config
   wins). Ships wired-up but empty. Override files are the user's own config, exempt from
   no-redistribution.
7. `patchelf --set-rpath '$ORIGIN'` (**without** `--force-rpath`) **only on bundled siblings that lack
   a RUNPATH** (e.g. `libgcc_s`, any bundled `libssl`/`libcrypto`). **Do NOT patchelf `libSz.so`** (it
   is ~430 MB and already RUNPATH=`$ORIGIN`; `--force-rpath` would downgrade it to legacy `DT_RPATH`,
   change precedence, and does not fix dlopen-by-soname). patchelf alone is insufficient — pair with
   step 4.

Assembly: `sbt -J-Xmx8g assembly` (`-J-Xmx4g` minimum; the ~464 MB payload → a **~265 MB** jar, dominated by the
already-stripped ~430 MB `libSz.so`; `stageNatives` also runs `strip` as a no-op safety net). Merge
strategy keeps `native/**` verbatim, discards META-INF signatures, concats services, last-wins
`reference.conf`. A post-assembly **verify** task asserts the jar contains exactly the
`readelf`-derived staged set. GLIBC floor (2.34) is published as a binding
deploy precondition and enforced at startup by `GlibcCheck`. Default ships x86_64 only (the licensed
dist available); aarch64 only if `stageNatives` sources a real aarch64 dist.

**Runtime self-extraction:** detect FAT-jar mode via the marker resource; extract once per node
(file-lock + `.ready`); ordered `System.load` then `SzCoreEnvironment.build()`; rewrite the three
PIPELINE paths; clean stale extractions; remove on shutdown unless `SENZING_KEEP_EXTRACTED=true`.

## 5. Spark jobs

Three data jobs + `RedoJob`, each its own `spark-submit` main from the same FAT jar; the three data
jobs share `SparkRecordOps`.

- **Partitioning:** read input → **force random partitioning** (`repartition(N)` round-robin or a
  random salt), N tuned to executor cores. **Never** sort/repartition by name/address/zip or any
  resolution key (2–10× throughput loss from lock contention).
- **Single-pass engine execution + two sinks (the only supported pattern):** engine calls are side
  effects that must run **exactly once**; two terminal writes of one lazy DataFrame would re-execute
  the lineage. So `mapPartitions` calls the engine once per record and emits one **tagged-union row**
  (good|error), written in a single action to a committer-backed staging table, then read back and
  split into output/error with zero re-execution. One commit per task attempt. Output committer keyed
  by **partitionId only** (never partition+attempt). `spark.speculation` **hard-disabled** for
  loader/delete.
- **Engine mutations are at-least-once** (explicit): the committer makes only output *files*
  exactly-once, not the repository. On task failure, applied `addRecord`/`deleteRecord` calls re-apply
  on re-run; `addRecord` is an idempotent replace so entity state converges, but each re-apply
  re-enqueues redo (absorbed by `RedoJob`).
- **ADD/UPDATE** (same verb): `addRecord(SzRecordKey.of(ds, recordId), recordJson, SZ_WITH_INFO)` →
  parse AFFECTED_ENTITIES → dedup into the per-task set → emit `AffectedEntityRow(op=ADD)` per unique
  entity at partition end.
- **DELETE**: `deleteRecord(SzRecordKey.of(ds, recordId), SZ_WITH_INFO)` → same. `deleteRecord`
  declares `SzUnknownDataSourceException, SzException` — it does **not** declare `SzNotFoundException`,
  so delete-of-absent behavior is decided by integration test, not assumed.
- **SEARCH**: `searchByAttributes(attributesJson, flags)` (overloads incl. a search-profile arg) →
  `SearchResultRow`; read-only, no redo.

**AFFECTED_ENTITIES semantics:** a change-**notification** feed of entity IDs only — no content, no
sequence/order id. Consumers build a settled view by **re-querying** `getEntity`/`getEntityByRecordId`
per affected id with last-write-wins. Per-task dedup is safe for the bounded loader feed; the unbounded
redo feed is not deduped upstream (see §7).

## 6. Worker loop + error taxonomy

`RecordWorker.run(partitionIterator)` on one task thread, single-threaded against the shared engine:

1. `acquire()` / `try … finally release()`. `ProgressLogger(prefix = "$dataSource/part-$partitionId")`,
   a `CircuitBreaker`, and a per-task affected-entity dedup set.
2. Per record: (a) `ConfigDrift.maybeReinit(env)` (throttled); (b) time the verb — if > LONG_RECORD
   threshold (default 300 s) log `LONG_RECORD` + counter, **never abort** (uncancellable JNI);
   (c) call the verb inside `withReadLock` with `SZ_WITH_INFO` / search flags; (d) on success parse
   AFFECTED_ENTITIES / results; (e) on exception classify **most-specific-subclass-first** against the
   verified hierarchy:
   - `SzUnknownDataSourceException` → CONFIG_RELEVANT: `forceCheckAndReinit`; retry once if config
     changed, else `ErrorRow` (matched before its `SzBadInputException` supertype).
   - `SzNotFoundException` → NOT_FOUND: benign skip **only if** `DeleteIT` proves `deleteRecord` throws
     it for an absent record; else routes through `SzBadInputException`.
   - `SzConfigurationException` → CONFIG_RELEVANT: force-reinit-and-retry-once.
   - `SzReplaceConflictException` → CONFIG_RELEVANT (not systemic): drift-check + retry once.
   - `SzBadInputException` (remaining) → BAD_INPUT: `ErrorRow`, continue.
   - `SzRetryTimeoutExceededException` → RETRY_EXHAUSTED: SDK already exhausted internal retries — do
     **not** re-enter worker backoff; `ErrorRow` directly (matched before `SzRetryableException`).
   - `SzRetryableException` (incl. `SzDatabaseConnectionLostException`/`SzDatabaseTransientException`) →
     CircuitBreaker check first (if tripped, rethrow to fail the partition fast); else jittered
     exponential backoff up to a budget; on exhaustion `ErrorRow(RETRY_EXHAUSTED)`, continue.
   - `SzUnrecoverableException` family + any other `SzException`/`Error` → SYSTEMIC: rethrow, **fail
     the task loudly**.
   - (f) every N records, `ProgressLogger` emits total + breakdown + **interval and cumulative** rate,
     from in-memory counters only (never `engine.getStats()`).
3. Partition end: flush deduped affected-entity rows; final summary.

## 7. Redo processing — THE DECISION

**Redo is its own Spark job that parallelizes processing like the other operations, with
`getRedoRecord()` as the source, run on a recurring schedule.** Two facts drive this:
processing must be **parallelized** (the documented `RedoContinuousViaFutures` / redo-worker-pool fan
records out to a processor pool), and **`getRedoRecord()==null` is not "done"** — the queue refills
from ongoing loads and from redo-begets-redo (the canonical worker pauses ~30 s on empty and resumes).
A single dedicated *one-shot single-consumer drain* is therefore rejected: it neither parallelizes nor
accounts for the refilling queue.

**Primary design — `RedoJob`, a scheduled parallel processor:**

- **Scheduled / recurring** (Databricks Job / Airflow / cron), or a long-lived job that pauses and
  resumes on empty. Each run drains what is currently pending; the schedule provides continuity.
- **Single dequeuer builds the DataFrame (the source):** a driver-side loop calls `getRedoRecord()` to
  pull a batch of redo-record JSON strings into a DataFrame. One consumer of the queue — this matches
  the canonical single-`getRedoRecord()`-thread pattern and **sidesteps concurrent multi-consumer
  dequeue, which the Senzing docs do not demonstrate.**
- **Parallel processing:** `repartition` randomly → `mapPartitions` → each task, single-threaded
  against the **shared per-executor engine**, calls `processRedoRecord(redoString, SZ_WITH_INFO)` and
  emits `AFFECTED_ENTITIES` (`op=REDO`) into the **same** affected-entities sink as the loaders. Reuses
  the same engine lifecycle, error taxonomy, LONG_RECORD handling, and progress logging as §5/§6.
- **Monitoring:** `countRedoRecords()` only as a coarse health gauge — never a loop condition.
- **Durability:** `getRedoRecord()` *removes* the record, so a dequeued record lives only in the
  DataFrame. Spark **task** retries are safe (the materialized source DataFrame replays); a **whole-job**
  crash after dequeue can lose the in-flight batch. Keep per-run batch sizes modest; redo is convergent
  (lost redo is typically regenerated) and the schedule re-drains. For strict durability, persist the
  pulled batch before processing.

**Rejected alternatives** (do not re-propose): per-task end-drain (drain redo at the end of every
loader subpartition — unpredictable task timing; relies on undemonstrated concurrent multi-consumer
`getRedoRecord()`); and a single-dequeuer one-shot drain (no parallelism; ignores the refilling queue).

> See `sz-spark-faq` → `redo/redo-strategy` for full rationale.

## 8. Testing

ScalaTest via `sbt test`. Tests fail loudly (no silent `println`); no mock-as-truth for Senzing ER.

- **Unit** (no engine/Spark): `ErrorTaxonomySpec` (real exception instances of every verified
  subclass, most-specific-first); `InfoParserSpec`; `BackoffSpec`; `CircuitBreakerSpec`;
  `ConfigDriftSpec` (throttle, write-lock quiesce, API on `SzEnvironment`); `EngineSettingsSpec`
  (three distinct PIPELINE dirs); `NativeBootstrapSpec` (lock+sentinel race, atomic extract, subtree
  preservation); `NativeLibLoaderSpec` (dependency-ordered load, libSz last); `GlibcCheckSpec`;
  `RedoSourceSpec` (the driver-side dequeuer pulls `getRedoRecord()` into a batch until null/limit;
  `countRedoRecords` never in a loop; processing fans out via `mapPartitions`); `InitConfigLockSpec` (first-init vs replace, no
  TOCTOU); `SchemaApplierSpec`; `SecondEnvBuildGuardSpec`; `RedoAdvisoryLockSpec`.
- **Concurrency**: `SzEngineProviderSpec` — many threads; env built once / destroyed once via the
  ordered hook; release-to-zero does **not** destroy; destroy runs before native cleanup.
- **Spark-local** (`local[*]`, fake engine): engine-calling stage executes exactly once though both
  output and error are produced; random repartition; option-B single staging write then read-back
  split; committer atomicity on systemic fail; per-task dedup; speculation disabled; `getStats` never
  per task.
- **Integration** (gated `SZ_IT=1`, real local Senzing + PostgreSQL; SQLite single-process dev smoke
  only; NO MOCK ER): `InitIT`, `LoadIT` (shuffled fixture; persists across JVM restart),
  `DeleteIT` (captures the exact delete-of-absent behavior that decides the NOT_FOUND branch),
  `SearchIT`, `RedoIT` (generates redo; the source dequeues into a DataFrame; parallel
  `processRedoRecord` emits op=REDO rows; a re-run is a convergent no-op; loaders leave redo undrained
  until RedoJob runs; a scheduled second run drains queue that refilled after the first),
  `ReplicationConsistencyIT`, `FatJarIT` (supported executor image, no system Senzing; proves the
  dlopen-by-soname fix with launch `LD_LIBRARY_PATH`), `BundleClosureIT` (build-time `readelf`
  closure assertion).
- **Regression**: any post-implementation bug gets a failing test committed first, then the fix.

## 9. Open risks (must be closed before this is final)

> **Verified against a real engine (closed):** #4 dlopen-by-soname — proven on a slim `temurin:21-jre`
> container with no system Senzing (FAT jar self-extracts + `SelfCheck` resolves entities); #6 DB-client
> contract — proven on real PostgreSQL (and SQLite); #12 delete-of-absent — observed to be a benign
> no-op (no `SzNotFoundException`), so `notFoundIsBenign=false` is correct (`EngineIT` regression).
> Also fixed: `isFatJar()` now requires the code source to be a jar file (the staged marker under
> `src/main/resources/` otherwise false-positives in dev/test). Still open: true multi-JVM/multi-node
> cluster run (local[N] is multi-thread), reinitialize() concurrency certification, MSSQL/MySQL.

1. **Scala/Spark pin** must match the target Databricks runtime exactly (Spark 4.0 → Scala 2.13;
   mismatch = runtime `NoSuchMethodError`). Confirm the cluster.
2. **Destroy-at-JVM-shutdown** assumes the shutdown hook fires; a SIGKILL skips `destroy()` (acceptable
   — process exit reclaims native resources — but no graceful teardown on hard kill).
3. **`reinitialize()` concurrency** is quiesced via the read/write lock and the API is corrected to
   `SzEnvironment`, but the Senzing-MCP does not explicitly certify `reinitialize()` concurrency safety
   even with verbs drained — obtain explicit Senzing confirmation before relying under heavy churn.
4. **dlopen-by-soname** is the highest-risk area. The fix requires launch-time
   `spark.executorEnv.LD_LIBRARY_PATH=<extract>/lib` + ordered `System.load`; must be proven by
   `FatJarIT` on the supported image with no system Senzing. If the launch env var cannot be set on a
   given Databricks cluster, the deploy fails — there is no post-JVM-start substitute.
5. **GLIBC floor (2.34)** must be confirmed against the actual target runtime and published as a
   binding precondition.
6. **DB-client image contract** is hard — the executor image must provide the chosen DB client's full
   closure; prove via `FatJarIT`/`BundleClosureIT`, never build-host `ldd`.
7. **CONFIGPATH** must point at the dedicated config tree (`cfgVariant.json`/`customGn|On|Sn.txt`/
   `defaultGNRCP.config`, under `er/resources/templates/`), not `er/resources/` root.
8. **Multi-arch**: only x86_64 is known available; advertise aarch64 only with a real sourced dist.
9. **Redo durability**: `getRedoRecord()` removes the record into the DataFrame, so Spark task retries
   are safe but a whole-job crash after dequeue can lose the in-flight batch. Keep per-run batches
   modest (redo is convergent and the schedule re-drains); persist the pulled batch first if strict
   durability is required.
10. **Read-consistency** requires consumers to re-query `getEntity` per affected id (IDs only, no
    sequence id). CONTINUOUS redo feed must not be entityId-deduped upstream; the bounded loader feed
    is deduped per task.
11. **Engine mutations are at-least-once**; the committer makes only output files exactly-once.
    `spark.speculation` must be hard-disabled for loader/delete.
12. **Delete-of-absent** classification is gated on a `DeleteIT`-observed fact (`deleteRecord` declares
    only `SzUnknownDataSourceException`/`SzException`).
13. **JSON attribute mapping** per data source must come from Senzing-MCP `mapping_workflow`; a wrong
    mapping silently degrades resolution rather than erroring.
14. **Exact V4 signatures and the `SzFlag` enum** were verified via `javap` this session but must be
    re-confirmed against the installed `sz-sdk.jar` at implementation time.
