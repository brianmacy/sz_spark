# Tutorial: sz_spark on Databricks

> ⚠️ **DRAFT — untested.** Not yet validated on a live Databricks workspace. Verify each step
> (especially the DBR/Spark version, GLIBC, and init-script package names) before relying on it.
> Cross-check Senzing specifics against the **Senzing-MCP** and your installed dist. This is the
> step-by-step walkthrough; `docs/DATABRICKS.md` is the condensed reference.

Runs the sz_spark jobs on Databricks against a co-located PostgreSQL (e.g. Aurora/RDS, or Azure DB
for PostgreSQL).

## Prerequisites

- A **DBR runtime whose Spark is 4.0.x / Scala 2.13** and JDK 17/21 (verify the DBR release notes —
  pin `scalaVersion` in `build.sbt` to match exactly). **GLIBC ≥ 2.34** (modern DBR images satisfy
  this; confirm).
- Licensed Senzing dist on the build host; FAT jar built locally (never published). Build per
  `docs/tutorials/spark-onprem.md` §1 and note `JAR_SHA`.
- A PostgreSQL reachable from the cluster's VNet/VPC (SSL on ⇒ `PGSSLMODE=require`).
- A **Unity Catalog Volume** (or DBFS path) to hold the jar.

## 1. Upload the jar

```bash
databricks fs cp target/scala-2.13/sz-spark-assembly.jar \
  dbfs:/Volumes/main/sz_spark/jars/sz-spark-assembly.jar
```

## 2. Store the engine config as a secret

```bash
databricks secrets create-scope sz
databricks secrets put-secret sz ecj   # paste SENZING_ENGINE_CONFIGURATION_JSON — use postgresql:// for
                                       # RDS/standard PG, aurorapostgresql:// for Aurora
```

## 3. Cluster configuration

**Environment variables** (set on driver AND workers — needed because the engine runs on executors for
data jobs and on the driver for `InitJob`):

```
SENZING_EXTRACT_DIR=/local_disk0/sz                      # local SSD; needs an SSD instance type. If the
                                                         # cluster has no /local_disk0, use /var/tmp (must be a real disk, not tmpfs).
LD_LIBRARY_PATH=/local_disk0/sz/sz-spark-JAR_SHA/lib      # MANDATORY — dlopen-by-soname; replace JAR_SHA
PGSSLMODE=require
SENZING_ENGINE_CONFIGURATION_JSON={{secrets/sz/ecj}}
```

**Spark config** (engine is native/off-heap — see `docs/RUNBOOK.md`):

```
spark.executor.cores 4
spark.executor.memory 2g
spark.executor.memoryOverhead 8g      # 4 GB engine + 1 GB per core (native)
spark.speculation false
```

**Cluster-scoped init script** (installs the DB-client + crypto libs the plugin dlopens; verify package
names for the DBR base OS):

```bash
#!/bin/bash
apt-get update -qq && apt-get install -y libpq5 libssl3   # provides libpq.so.5 + libssl.so.3
```

Pick **memory-optimized** workers sized so each node covers `executor.memory + memoryOverhead` per
executor. Use a fixed-size cluster (or pin the autoscaling) so the `JAR_SHA` extract path is stable.

## 4. One-time init

Run `InitJob` **once** before any load — as a **JAR task on a single-node cluster** (it needs the
engine on the driver, but NOT Spark) or a notebook. That single-node cluster MUST have the same §3
setup (env vars incl. `LD_LIBRARY_PATH`/`SENZING_EXTRACT_DIR`/`SENZING_ENGINE_CONFIGURATION_JSON`, and
the init script): `InitJob` runs `GlibcCheck` + native self-extraction on the driver before the JDBC
step, so it fails loudly if those aren't in place. It applies the schema DDL + registers the default
config. Never run it inside a data job (two engines in one process crash).

JAR task: main class `com.senzing.spark.jobs.InitJob`, parameters:
```
dialect=postgresql
db=jdbc:postgresql://DBHOST:5432/G2?sslmode=require&user=USER&password=PASS
dataSources=CUSTOMERS
```

## 5. Jobs as a Databricks Workflow

Create a multi-task Job (Workflows), each task a **JAR task** on the configured cluster:

| Task | Main class | Key parameters |
|---|---|---|
| add-update | `com.senzing.spark.jobs.AddUpdateJob` | `input=… output=… errors=… staging=… dataSource=CUSTOMERS partitions=N` |
| delete | `com.senzing.spark.jobs.DeleteJob` | same shape |
| search | `com.senzing.spark.jobs.SearchJob` | `input=<requests.jsonl> output=… …` |

Order the loaders after init (hard dependency). Add a **separate, scheduled Job** for
`com.senzing.spark.jobs.RedoJob` (e.g. every few minutes) — `getRedoRecord()==null` is not "done"
(the queue refills); run one RedoJob instance at a time.

Read paths can be UC Volumes / cloud storage (`s3://`, `abfss://`, `/Volumes/…`).

## Gotchas

- **`LD_LIBRARY_PATH` must be set as a cluster env var** (driver+workers) to the `sz-spark-<JAR_SHA>/lib`
  extract dir — `$ORIGIN` does not cover dlopen-by-soname. If it can't be set, the deploy fails.
- Pin **Scala 2.13.x** to the DBR's Spark 4.0 build exactly (mismatch under `provided` = runtime
  `NoSuchMethodError`).
- Size **`memoryOverhead`**, not heap, for the native engine (4 GB + 1 GB/core).
- Don't partition input by a resolution key; partition randomly (`partitions=N`).
- Validate first with `com.senzing.spark.diag.SelfCheck` as a JAR task on a single-node cluster (env
  vars set) before running a full load.
- The FAT jar bundles the Senzing SDK — keep it in your own Volume/workspace; never publish it.

See `docs/DATABRICKS.md` (reference) and `docs/DESIGN.md` (architecture).
