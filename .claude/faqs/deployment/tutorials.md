# Deployment tutorials (on-prem / EMR / Databricks)

Step-by-step deploy walkthroughs live under `docs/tutorials/`:

- `docs/tutorials/spark-onprem.md` — standalone / YARN cluster.
- `docs/tutorials/aws-emr.md` — AWS EMR + Aurora/RDS PostgreSQL.
- `docs/tutorials/databricks.md` — Databricks Jobs/Workflows.

**All three are ⚠️ DRAFT — untested end-to-end on a real cluster.** Verify before relying on them.

They share the same binding facts (see `RUNBOOK.md` / `DATABRICKS.md` for the reference):

- Build the FAT jar locally from a licensed dist (`sbt stageNatives && sbt -J-Xmx4g assembly`); note its
  `sha256` — the self-extraction dir is `$SENZING_EXTRACT_DIR/sz-spark-<sha>/`.
- **Set `LD_LIBRARY_PATH=<extractDir>/lib` at executor LAUNCH** (`spark.executorEnv...` / cluster env) —
  mandatory for dlopen-by-soname; no post-JVM-start substitute.
- **Size `spark.executor.memoryOverhead ≈ 4 GB + 1 GB/core`** (native/off-heap), not JVM heap.
- **GLIBC ≥ 2.34** on workers (EMR: use 7.x/AL2023, not 6.x/AL2). Each worker needs the DB-client libs
  (`libpq.so.5` for PG) + OpenSSL 3.
- Run **`InitJob` once** (schema DDL + default config + data sources) before the load jobs; never inside
  a Spark job.
- Engine config via `SENZING_ENGINE_CONFIGURATION_JSON` (`postgresql://user:pass@host:port/database`),
  `PGSSLMODE=require` for cloud PG. Aurora: IO-Optimized, same-AZ, `synchronous_commit=off` for bulk
  loads, `max_connections >= cores×(executors+1)+overhead`.
- Run **`RedoJob` on a schedule** (recurring; the queue refills), one instance at a time.
- Never publish the FAT jar (bundles the SDK).
