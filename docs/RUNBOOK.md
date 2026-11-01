# sz_spark — Operations Runbook

Binding preconditions and operational guidance. See `docs/DESIGN.md` for rationale and `docs/IMPLEMENTATION_PLAN.md` for the build.

## Binding deploy preconditions

- **Licensed Senzing dist** at build time (`$SENZING_DIR`, default `/opt/senzing`). The SDK jar +
  native libs are **not** redistributable — build the FAT jar locally; never publish it.
- **GLIBC ≥ 2.34** on every executor image (verified floor; `GlibcCheck` enforces at startup). Confirm
  against your actual Databricks runtime.
- **Native loading needs no launch-time `LD_LIBRARY_PATH` (FAT jar).** The engine `System.load`s the
  helpers `libSz.so` dlopens by soname (`libgcc_s`, `libszzstd`, `libszvec`) in dependency order, then
  defines the SDK implementation (`com.senzing.sdk.core`) through a class loader whose `findLibrary`
  answers from `<extractDir>/lib`, so the SDK's own `System.loadLibrary("Sz")` finds `libSz.so` there
  (`SzEnvironmentFactory.nativeAware`). Setting `spark.executorEnv.LD_LIBRARY_PATH=<extractDir>/lib` at
  executor launch still works exactly as before and remains the way to go when you stage the DB
  client closure into `lib/` (air-gapped variant: those libraries are not preloaded). A non-FAT-jar run
  against an installed dist (`InitJob`, dev runs) still needs `LD_LIBRARY_PATH=$SENZING_DIR/er/lib`.
- **DB-client closure on the executor image** for your dialect (PostgreSQL: `libpq.so.5` + chain; MSSQL:
  ODBC; MySQL: `libmysqlclient`), or stage it into `lib/` (air-gapped variant) and rely on the launch
  `LD_LIBRARY_PATH`.
- **Engine config**: `SENZING_ENGINE_CONFIGURATION_JSON` provided as an env/secret (never hardcoded),
  or an explicit value passed from the driver to the engine-facing cores (`AddCore.run(...,
  engineConfig = Some(json))`, likewise `DeleteCore`/`SearchCore`/`RedoCore`/`GetCore`/`DepartedVerify`)
  which travels to the executors in the task closure and wins over the env var. One environment per
  executor JVM, so every task in a JVM must use the same config (a different value fails loudly). Set
  `PGSSLMODE=require` for cloud Postgres.

## Executor memory sizing (CRITICAL — Senzing is native/off-heap)

The engine is native C; its working memory is **off-heap**, so it must be reserved as
**`spark.executor.memoryOverhead`**, not `spark.executor.memory`. Under-size it and the cluster
manager OOM-kills the container under load. Rule of thumb per executor JVM:

```
native RAM ≈ 4 GB (base engine) + 1 GB × spark.executor.cores   # 1 GB per concurrent Sz thread
```

```
spark.executor.cores          = N
spark.executor.memory         = 2g          # JVM heap — modest (records stream)
spark.executor.memoryOverhead = (4 + N) g   # engine base + per-thread native + headroom
```

Memory scales with the **same knob** as concurrency (cores per executor). Prefer more executors with
moderate cores over a few fat executors if per-node native RAM is tight. See
`sz-spark-faq` → `deployment/executor-memory-sizing`.

## FAT jar size

~**265 MB** (≈464 MB staged payload, dominated by the already-stripped ~430 MB `libSz.so`) — not
multi-GB. Build with `-J-Xmx8g` (`-J-Xmx4g` minimum).

## One-time init (before any Spark job)

`InitJob` is a separate run-once JVM step (never an executor): applies the schema DDL (PostgreSQL/MySQL/
MSSQL; SQLite auto-creates) and registers the default config with exactly one environment. Re-run it if
a config override changes the registered config (data sources / features / ER rules).

## Running the jobs

Each is its own `spark-submit` application from the one FAT jar; concurrency = executor/worker count
(no threads-per-worker knob). `spark.speculation` is hard-disabled for loader/delete (engine calls are
side effects). **Do not** partition input by a resolution key (name/address/zip) — partition randomly.

- **Add/update** → `AddUpdateJob` (`addRecord` WITH_INFO) → deduped affected-entity output + errors.
- **Delete** → `DeleteJob`.
- **Search** → `SearchJob` (request + result rows).
- **Redo** → `RedoJob`, run on a **schedule** (the redo queue refills; `getRedoRecord()==null` is not
  "done"). A single driver-side dequeuer feeds parallel `processRedoRecord` across tasks.

Args: `input=… output=… errors=… staging=… partitions=N runId=… [redoBatch=N]`. There is **no**
per-job `dataSource=` load argument — each record supplies its own `DATA_SOURCE` and `RECORD_ID` in its
JSON body (both required; a record missing either is dead-lettered as `BAD_INPUT` without an engine
call). `InitJob`'s `dataSources=` registration argument is separate and unaffected.

## Reading results (consistency)

`AFFECTED_ENTITIES` is a change-**notification** feed of entity ids (no content, no sequence id).
Consumers build a settled view by **re-querying** `getEntity`/`getEntityByRecordId` per affected id with
last-write-wins. Engine mutations are **at-least-once** (the committer makes only output files
exactly-once); a re-run re-applies `addRecord` (idempotent replace) and regenerates redo (absorbed by
`RedoJob`).

## Monitoring

`countRedoRecords()` is a coarse health gauge only — **never** a loop condition. Watch the per-task
progress lines (interval + cumulative rate) and the `LONG_RECORD` warnings (uncancellable JNI verbs on
large-entity merges — never aborted).
