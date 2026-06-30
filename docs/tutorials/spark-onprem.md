# Tutorial: sz_spark on an on-premise Spark cluster

> ⚠️ **DRAFT — untested.** This tutorial has not yet been validated end-to-end on a real multi-node
> cluster. Treat every command as a starting point to verify, not a guarantee. Confirm Senzing
> specifics against the **Senzing-MCP** and your installed dist before relying on it.

Deploys the sz_spark jobs to a standalone or YARN Spark cluster against a co-located PostgreSQL.

## Prerequisites

- A **licensed Senzing dist** on the build host (`$SENZING_DIR`, default `/opt/senzing`). The SDK jar +
  native libs are not redistributable — build the FAT jar locally; never publish it.
- Spark **4.0.x**, JDK **17/21**, Scala **2.13** on the cluster; **GLIBC ≥ 2.34** on every worker.
- A reachable **PostgreSQL** (co-located, same subnet/low latency to the workers).
- Each worker must provide the chosen DB client closure on its loader path — for PostgreSQL,
  `libpq.so.5` (e.g. `apt install libpq5`). `libSz` also needs OpenSSL 3 (`libssl3`).

## 1. Build the FAT jar (build host)

```bash
export SENZING_DIR=/opt/senzing
sbt stageNatives          # stage libs/data/resources/config from the licensed dist (gitignored)
sbt -J-Xmx8g assembly     # -> target/scala-2.13/sz-spark-assembly.jar  (~265 MB; -J-Xmx4g minimum)
JAR_SHA=$(sha256sum target/scala-2.13/sz-spark-assembly.jar | cut -d' ' -f1)
echo "$JAR_SHA"           # used below to set the launch LD_LIBRARY_PATH
```

The jar self-extracts its native payload on each node at first use (once per node, file-locked) into
`$SENZING_EXTRACT_DIR/sz-spark-$JAR_SHA/` (default `$SENZING_EXTRACT_DIR=/var/tmp`).

## 2. Provision PostgreSQL

Apply tuning that matters for Senzing (verify against Senzing-MCP `search_docs "database tuning"`):

- `synchronous_commit = off` during bulk loads (large throughput win), re-enable after.
- `max_connections >= spark.executor.cores × (num_executors + 1) + overhead`. Each Senzing engine
  thread (= one Spark task slot = one core) holds a DB connection for its lifetime.
- Co-locate the DB with the workers (latency compounds over millions of round-trips).

## 3. Engine config + one-time init

```bash
# CONFIGPATH = the Senzing config dir (cfgVariant.json / customGn|On|Sn.txt / g2config.json). On a
# standard install that's /etc/opt/senzing; an sz_create_project layout uses its own etc/ dir.
# RESOURCEPATH and SUPPORTPATH are distinct trees. (In FAT-jar mode the runtime rewrites all three.)
export SENZING_ENGINE_CONFIGURATION_JSON='{
  "PIPELINE":{"CONFIGPATH":"/etc/opt/senzing",
              "RESOURCEPATH":"/opt/senzing/er/resources",
              "SUPPORTPATH":"/opt/senzing/data"},
  "SQL":{"CONNECTION":"postgresql://USER:PASS@DBHOST:5432/G2"}}'
export PGSSLMODE=require   # for managed/cloud PG; "disable" only for a local no-SSL dev PG
export LD_LIBRARY_PATH=/opt/senzing/er/lib   # on the (build/admin) host that has the dist

# Run ONCE (separate process — never inside a Spark job): applies schema DDL + registers default config.
java -cp target/scala-2.13/sz-spark-assembly.jar com.senzing.spark.jobs.InitJob \
  dialect=postgresql db="jdbc:postgresql://DBHOST:5432/G2?user=USER&password=PASS&sslmode=require" \
  dataSources=CUSTOMERS
```

`InitJob` applies `szcore-schema-postgresql-create.sql` via JDBC (idempotent — skips if `sys_cfg`
exists), then registers the default config + your data source(s). The Spark jobs assume both exist.

## 4. Distribute the jar + set the launch environment

Copy the jar to a path readable by every node (shared mount or `--files`/`--jars`). The **critical**
cluster settings (engine is native/off-heap; `libSz` dlopens plugins by soname):

```bash
spark-submit \
  --master spark://MASTER:7077 \                 # or --master yarn --deploy-mode cluster
  --class com.senzing.spark.jobs.AddUpdateJob \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memory=2g \              # JVM heap — modest; records stream
  --conf spark.executor.memoryOverhead=8g \      # NATIVE/off-heap: 4 GB engine + 1 GB/core
  --conf spark.speculation=false \               # engine calls are side effects (exactly-once)
  --conf spark.executorEnv.LD_LIBRARY_PATH="/var/tmp/sz-spark-$JAR_SHA/lib" \   # MANDATORY (dlopen-by-soname)
  --conf spark.executorEnv.SENZING_EXTRACT_DIR=/var/tmp \
  --conf spark.executorEnv.PGSSLMODE=require \
  --conf spark.yarn.appMasterEnv.SENZING_ENGINE_CONFIGURATION_JSON="$SENZING_ENGINE_CONFIGURATION_JSON" \
  --conf spark.executorEnv.SENZING_ENGINE_CONFIGURATION_JSON="$SENZING_ENGINE_CONFIGURATION_JSON" \
  target/scala-2.13/sz-spark-assembly.jar \
  input=hdfs:///data/customers.jsonl output=hdfs:///out/affected errors=hdfs:///out/errors \
  staging=hdfs:///staging/addupdate dataSource=CUSTOMERS partitions=64
```

**Why `memoryOverhead` not heap:** Senzing is native C; its working set is off-heap. Size it
`≈ 4 GB + 1 GB × spark.executor.cores`; under-sizing gets executors OOM-killed. Memory scales with the
same cores knob as concurrency.

**The launch `LD_LIBRARY_PATH` is non-negotiable** — `$ORIGIN` does not cover the libraries `libSz`
`dlopen`s by soname (DB plugin, etc.). It must be set at executor launch; there is no post-start fix.

## 5. Run the other jobs

```bash
# Delete:  --class com.senzing.spark.jobs.DeleteJob   ... dataSource=CUSTOMERS
# Search:  --class com.senzing.spark.jobs.SearchJob   input=.../requests.jsonl ...
# Redo:    --class com.senzing.spark.jobs.RedoJob      output=... staging=... partitions=64 redoBatch=100000
```

Run **`RedoJob` on a schedule** (cron / Airflow) — `getRedoRecord()==null` is not "done"; the queue
refills from ongoing loads. One RedoJob instance at a time.

## Gotchas

- **Do not** repartition input by name/address/zip (any resolution key) — it causes lock contention.
  Partition randomly (`partitions=N`).
- **No SQLite** for a real cluster (no concurrent writes) — PostgreSQL/MSSQL/MySQL only.
- Confirm **GLIBC ≥ 2.34** on every worker (`GlibcCheck` fails the task loudly otherwise).
- Each worker needs the **DB-client libs** (`libpq.so.5` for PG) and **OpenSSL 3** (`libssl3`).
- Self-check a node without launching a full job (the data source must already be registered by
  `InitJob`):
  `LD_LIBRARY_PATH=/var/tmp/sz-spark-$JAR_SHA/lib java -cp sz-spark-assembly.jar com.senzing.spark.diag.SelfCheck CUSTOMERS`

See `docs/RUNBOOK.md` for binding preconditions and `docs/DESIGN.md` for the architecture.
