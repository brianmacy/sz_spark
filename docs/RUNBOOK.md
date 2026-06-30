# sz_spark — Operations Runbook

Binding preconditions and operational guidance. See `docs/DESIGN.md` for rationale and `docs/IMPLEMENTATION_PLAN.md` for the build.

## Binding deploy preconditions

- **Licensed Senzing dist** at build time (`$SENZING_DIR`, default `/opt/senzing`). The SDK jar +
  native libs are **not** redistributable — build the FAT jar locally; never publish it.
- **GLIBC ≥ 2.34** on every executor image (verified floor; `GlibcCheck` enforces at startup). Confirm
  against your actual Databricks runtime.
- **`spark.executorEnv.LD_LIBRARY_PATH=<extractDir>/lib` set at executor LAUNCH.** This is mandatory:
  `libSz.so` dlopens `libszvec.so`/`libszzstd.so`/the DB plugin **by soname**, which `$ORIGIN`/RUNPATH
  does not resolve. There is **no** post-JVM-start substitute — if the cluster cannot set this launch
  env var, the deploy fails by design.
- **DB-client closure on the executor image** for your dialect (PostgreSQL: `libpq.so.5` + chain; MSSQL:
  ODBC; MySQL: `libmysqlclient`), or stage it into `lib/` (air-gapped variant) and rely on the launch
  `LD_LIBRARY_PATH`.
- **`SENZING_ENGINE_CONFIGURATION_JSON`** provided as an env/secret (never hardcoded). Set
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
multi-GB. Build with `-J-Xmx4g`.

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

Args: `input=… output=… errors=… staging=… dataSource=… partitions=N runId=… [redoBatch=N]`.

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
