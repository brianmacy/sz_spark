# Examples: end-to-end Senzing on Spark

Copy-paste examples for running the jobs: initialize, add/update, search, process redo, and view the
output / error DataFrames. The job arguments and main classes here are taken directly from the source
(`com.senzing.spark.jobs.*`). For the **validated** local end-to-end run, use
[`scripts/it-local.sh`](../scripts/it-local.sh) (real engine on SQLite); cluster `spark-submit`
invocations follow the same documented arguments. See [`DATABRICKS.md`](DATABRICKS.md) for the
Databricks form and [`ARCHITECTURE.md`](ARCHITECTURE.md) for what each job does.

> ⚠️ **Only the local `scripts/it-local.sh` path is exercised end-to-end.** The `spark-submit`
> command lines below are constructed from the verified job arguments but have **not** been run as
> shown on a cluster — treat them as documented invocations, not a tested recipe.

## Common arguments and conventions

All four data jobs parse simple `key=value` arguments:

| Arg | Meaning | Default |
|---|---|---|
| `input` | input path (**JSONL**; search reads JSONL search requests) | — |
| `output` | output DataFrame path (**Parquet**) | `output` |
| `errors` | error DataFrame path (Parquet) | `errors` |
| `staging` | staging table path (single-write-then-split) | `staging` |
| `dataSource` | the registered data source code for add/delete | — |
| `partitions` | random repartition count (tune to total executor cores) | `0` (no repartition) |
| `runId` | tag stamped on output rows | `run` |
| `redoBatch` | redo dequeue batch size (RedoJob only) | `100000` |

- **Input is JSONL**, output/errors are **Parquet**.
- The engine config comes from `SENZING_ENGINE_CONFIGURATION_JSON` (never hardcoded). Set
  `PGSSLMODE=require` for cloud Postgres.
- Concurrency = executor cores; partition **randomly**, never by a resolution key.

## 0. Environment and engine configuration JSON

```bash
export SENZING_ENGINE_CONFIGURATION_JSON='{"PIPELINE":{...},"SQL":{"CONNECTION":"postgresql://user:pass@db-host:5432:G2/"}}'
export PGSSLMODE=require                     # cloud Postgres
JAR=target/scala-2.13/sz-spark-assembly.jar  # built per docs/BUILD.md
```

> Generate the JSON attribute mapping for your records with the Senzing-MCP `mapping_workflow` — a
> wrong mapping silently degrades resolution rather than erroring.

## 1. Initialize the repository (InitJob, run once)

`InitJob` applies the schema DDL and registers the default config + data sources. It is a standalone
JVM step — **never** run it on an executor.

```bash
# dialect: postgresql (default) | mysql | mssql ; SQLite auto-creates (omit db=)
java -cp "$JAR" com.senzing.spark.jobs.InitJob \
  dialect=postgresql \
  db='jdbc:postgresql://db-host:5432/G2?sslmode=require' \
  dataSources=CUSTOMERS,WATCHLIST
```

- **`db=<jdbcUrl>` is required for postgresql/mysql/mssql** — the schema-DDL step runs only when both
  a non-SQLite `dialect` and `db=` are present (`InitJob.scala`). Omit `db=` only for SQLite, which
  auto-creates the schema. Without `db=` on Postgres/MySQL/MSSQL the schema is **silently skipped**,
  and later data jobs fail as "engine not initialized" (see [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md)).
- `SENZING_DIR` (default `/opt/senzing`) locates the schema DDL file + native libs/config for the init
  step; `SENZING_ENGINE_CONFIGURATION_JSON` supplies the engine config for config registration.
- Re-run `InitJob` if a config override changes the registered config (data sources / features / ER
  rules).

## 2. Add/update records (AddUpdateJob, spark-submit)

`addRecord` per record with `SZ_WITH_INFO`; emits deduped **affected entity IDs** + an error frame.

```bash
spark-submit --class com.senzing.spark.jobs.AddUpdateJob \
  --conf spark.executorEnv.LD_LIBRARY_PATH=/var/tmp/<sha>/lib \
  --conf spark.speculation=false \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memory=2g \
  --conf spark.executor.memoryOverhead=8g \
  "$JAR" \
  input=/data/customers.jsonl \
  dataSource=CUSTOMERS \
  output=/out/affected \
  errors=/out/errors \
  staging=/out/staging \
  partitions=64 \
  runId=load-2026-06-30
```

`memoryOverhead ≈ (4 + cores) GB` (see [`PERFORMANCE.md`](PERFORMANCE.md)). `LD_LIBRARY_PATH` points at
the per-jar extract dir's `lib/` — `$SENZING_EXTRACT_DIR/<sha>/lib`, where `SENZING_EXTRACT_DIR`
defaults to `/var/tmp` and `<sha>` is the per-jar hash — and **must be set at launch**
(dlopen-by-soname). `DeleteJob` is identical with `--class com.senzing.spark.jobs.DeleteJob`.

## 3. Search (SearchJob, spark-submit)

`searchByAttributes` per request; emits **request paired with results** + an error frame. Read-only
(no redo).

```bash
spark-submit --class com.senzing.spark.jobs.SearchJob \
  --conf spark.executorEnv.LD_LIBRARY_PATH=/var/tmp/<sha>/lib \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memoryOverhead=8g \
  "$JAR" \
  input=/data/search-requests.jsonl \
  output=/out/search-results \
  errors=/out/search-errors \
  staging=/out/search-staging \
  partitions=64 \
  runId=search-2026-06-30
```

## 4. Process redo (RedoJob, scheduled/recurring)

`RedoJob` dequeues `getRedoRecord()` into a DataFrame (driver-side, single consumer) and processes it
in parallel, emitting affected-entity rows with `op=REDO`. Run it on a **schedule** (the queue
refills); run **one** instance at a time; `getRedoRecord()==null` is **not** "done".

```bash
spark-submit --class com.senzing.spark.jobs.RedoJob \
  --conf spark.executorEnv.LD_LIBRARY_PATH=/var/tmp/<sha>/lib \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memoryOverhead=8g \
  "$JAR" \
  output=/out/redo-affected \
  errors=/out/redo-errors \
  staging=/out/redo-staging \
  partitions=64 \
  redoBatch=100000 \
  runId=redo-2026-06-30
```

## 5. View the output and error DataFrames (ShowOutput)

```bash
# print an affected-entity / error Parquet path (mutating jobs: add/update/delete/redo)
spark-submit --class com.senzing.spark.diag.ShowOutput "$JAR" /out/affected
```

`ShowOutput` is for the **affected-entity / error** output of the mutating jobs — it reports the
`entityId` column and so does not apply to search output (`SearchResultRow` has only
`requestJson` / `resultJson`).

Output (`AffectedEntityRow`) columns: `dataSource, recordId, entityId, op{ADD,DELETE,REDO}, runId`.
Error (`ErrorRow`) columns: `dataSource, recordId, payload, category, errorCode, message, attempts`.

`AFFECTED_ENTITIES` is a change-**notification** feed (IDs only). To get settled entity content,
**re-query** `getEntity` / `getEntityByRecordId` per affected ID with last-write-wins
(see [`RUNBOOK.md`](RUNBOOK.md) "Reading results").

## 6. Single-node self-check (SelfCheck diagnostic)

Confirm native loading + init + a real resolve/search end-to-end on one node before scaling out:

```bash
spark-submit --class com.senzing.spark.diag.SelfCheck "$JAR" TEST    # arg = data source (default TEST)
```

## Validated local run (real engine, SQLite)

```bash
./scripts/it-local.sh
```

This is the **exercised** end-to-end path: it stands up a real engine on SQLite, runs `InitJob`, then
`EngineIT` (real `addRecord` / `searchByAttributes` / redo through the Spark jobs). Use a co-located
**PostgreSQL** for anything beyond a single-process smoke — SQLite is dev-only (no concurrent writes).
