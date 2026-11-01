# Troubleshooting Senzing on Spark/Databricks

Spark-specific failure modes for running the Senzing V4 SDK on executors, and how to diagnose them.
Most failures are **native-library loading**, **license/runtime mismatch**, **database connections**,
or **off-heap memory**. See [`RUNBOOK.md`](RUNBOOK.md) for the binding preconditions and
[`PERFORMANCE.md`](PERFORMANCE.md) for sizing.

## Native library fails to load on the executor

**Symptom:** `UnsatisfiedLinkError` / `java.lang.UnsatisfiedLinkError: ... libszvec.so` /
`cannot open shared object file` when the engine first initializes on an executor — even though the
FAT jar built fine.

**Root cause:** `libSz.so` `dlopen`s several siblings (`libszvec.so`, `libszzstd.so`, and the DB
plugin) **by soname**. `$ORIGIN` / RUNPATH only resolves a library's **direct NEEDED** entries — it
does **not** apply to dlopen-by-soname. So bundling the libs and `patchelf`-ing `$ORIGIN` is **not
sufficient** on its own.

**How the engine handles it (FAT jar):**
1. `NativeLibLoader.loadHelpers` `System.load()`s the helpers in dependency order so each later dlopen
   finds an already-loaded handle: `libgcc_s.so.1 → libszzstd.so → libszvec.so`.
2. `libSz.so` itself is loaded by the SDK's own `System.loadLibrary("Sz")`, which finds it through
   the native-aware class loader that defines `com.senzing.sdk.core` (`SzEnvironmentFactory.nativeAware`,
   whose `findLibrary` answers from `<extractDir>/lib`). No launch-time `LD_LIBRARY_PATH` is needed.

If you still see this error: check that nothing else in the JVM loaded the SDK implementation first
(JNI binds `libSz.so` to ONE class loader), and that the DB plugin's own dependencies resolve — set
`spark.executorEnv.LD_LIBRARY_PATH=<extractDir>/lib` at executor launch when you stage the DB client
closure into `lib/` (it still works exactly as before). `InitJob` and other non-FAT-jar runs load the SDK
directly and need `LD_LIBRARY_PATH` pointing at the dist's `lib/`.

This is the highest-risk area; `NativeLoaderIT` (scripts/it-local.sh) proves the loader path with
`LD_LIBRARY_PATH` unset. See [`BUILD.md`](BUILD.md) and FAQ `build/fat-jar-and-native-extraction`.

## "GLIBC_2.xx not found" / glibc too old

**Symptom:** `version 'GLIBC_2.34' not found` at native load, or `GlibcCheck` fails loudly at startup.

**Root cause:** the executor image's glibc is older than the verified floor (**2.34**).

**Fix:** use an executor/DBR image with **glibc ≥ 2.34**. `GlibcCheck` enforces this *before* any native
load so you get a clear message instead of a cryptic linker error. Confirm the floor against your actual
Databricks runtime.

## Database client library missing on the executor

**Symptom:** native load or first DB call fails resolving `libpq.so.5` (PostgreSQL), an ODBC driver
(MSSQL), or `libmysqlclient` (MySQL).

**Root cause:** the DB plugin is dlopen'd by soname and needs the DB client's **full closure** on the
loader path; the executor image doesn't provide it.

**Fix:** install the DB client closure on the executor image, **or** use the air-gapped FAT-jar variant
that stages the client into `lib/` and relies on the launch `LD_LIBRARY_PATH`. Never assume the
build-host's `ldd` closure matches the executor image.

## "Engine not initialized" / second-environment crash

**Symptom:** intermittent SIGSEGV or a crash ~6 minutes in; or engine calls fail as if uninitialized.

**Root causes & fixes:**
- **Two `SzEnvironment` instances in one process.** Building a second environment (e.g. running
  `InitJob` logic inside an executor, or rebuilding after destroy) corrupts the C library. Init is a
  **separate run-once JVM step**, never on an executor. One engine per executor JVM, built once.
- **Init never ran.** The schema is **not** auto-created for PostgreSQL/MySQL/MSSQL. Run `InitJob`
  before any data job (see [`EXAMPLES.md`](EXAMPLES.md)).
- **Destroyed-then-rebuilt in a living JVM.** Don't refcount-to-zero-destroy; `acquire`/`release` are a
  liveness counter only. Databricks reuses executor JVMs across stages.

## Executors OOM-killed under load

**Symptom:** executors die with no Java `OutOfMemoryError` in the heap dump; the cluster manager reports
the container was killed (exit 137 / "killed by YARN/K8s for exceeding memory").

**Root cause:** Senzing's working set is **native/off-heap**. If you sized `spark.executor.memory`
(heap) instead of `spark.executor.memoryOverhead`, the container exceeds its memory limit and is killed.

**Fix:** reserve off-heap memory as `spark.executor.memoryOverhead ≈ (4 + cores) GB` (≈ 4 GB base
engine + 1 GB per core). Keep heap modest (`spark.executor.memory = 2g`). → FAQ
`deployment/executor-memory-sizing`.

## Database connection-pool exhaustion

**Symptom:** under high parallelism, records fail with connection / "too many clients" errors;
`SzRetryableException` / `SzDatabaseConnectionLostException` spikes.

**Root cause:** **each Senzing engine thread holds one DB connection for its lifetime.** The cluster's
total cores (plus init/redo/admin) exceeded the database's `max_connections`.

**Fix:** size `max_connections ≥ Σ(executor.cores) + redo + init + admin + headroom`. See
[`PERFORMANCE.md`](PERFORMANCE.md) "Connection planning." Reduce executor cores if you cannot raise the
DB limit.

## Throughput is far below expectations

**Likely causes, in order** (matches Senzing's general guidance):
1. **Database not tuned** — `synchronous_commit` still on, or synchronous replication re-imposing the
   commit flush. Run `check_repository_performance()`; it is the ceiling.
2. **Database not co-located** — cross-AZ/region latency compounding across commits.
3. **Input partitioned by a resolution key** — lock contention; partition **randomly** instead.
4. **Too few executor cores**, or DB already saturated (adding cores then only adds contention).

See [`PERFORMANCE.md`](PERFORMANCE.md). Check `engine.getStats()` contention fields and per-task
records/sec to localize the bottleneck.

## A single record runs for a very long time

**Symptom:** a task stalls; logs show `LONG_RECORD` past the threshold (default 300 s).

**Root cause:** large-entity merges are genuinely slow, and Senzing verbs are **synchronous,
uncancellable JNI** calls. **Never abort or relocate a running record** — it is logged but allowed to
finish. → FAQ `troubleshooting/slow-records`.

## Duplicate / re-applied work after a task retry

**Symptom:** redo volume higher than expected after a task or job retry.

**Root cause (by design):** engine mutations are **at-least-once** — the committer makes only output
*files* exactly-once, not the repository. A re-run re-applies `addRecord` (idempotent replace, so
entity state converges) and regenerates redo, absorbed by `RedoJob`. Ensure `spark.speculation=false`
for loader/delete jobs (it is hard-disabled in code) so the same record isn't run by two task attempts
concurrently.

## `Spark version` / `NoSuchMethodError` at runtime

**Symptom:** the job compiles but throws `NoSuchMethodError` / `NoClassDefFoundError` on the cluster.

**Root cause:** Scala/Spark version mismatch — the FAT jar was built for a different Scala (2.12 vs
2.13) or Spark minor than the Databricks runtime, under `provided` scope.

**Fix:** pin `scalaVersion` and the Spark version to the **exact** target DBR (Spark 4.0 → Scala 2.13).

## Quick diagnostic

Run the bundled self-check on a single node to confirm native loading, init, and a real
resolve/search end-to-end before scaling out:

```bash
spark-submit --class com.senzing.spark.diag.SelfCheck sz-spark-assembly.jar TEST
```

See [`EXAMPLES.md`](EXAMPLES.md) for the full diagnostic and job commands.
