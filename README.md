# sz_spark

**Run [Senzing](https://senzing.com) entity resolution at scale on Apache Spark — from any record
source, into any datastore, with the resolved entities replicated wherever you need them.**

sz_spark packages the Senzing V4 SDK and its native libraries into one self-extracting **FAT jar** that
runs as ordinary Spark jobs — no `/opt/senzing` install on your cluster. It's a runnable reference
implementation: a small core (the engine on Spark) between two seams you adapt — where records come
**in**, and where resolved entities go **out**.

```mermaid
flowchart LR
  src["Your source<br/>Kafka · RabbitMQ · Delta · files"] -->|RecordSource| eng["Senzing engine<br/>on Spark"]
  eng -->|affected-entity feed| store["Your entity store<br/>Delta · warehouse · online"]
```

## What it does

- **Resolve records at scale** — add / update / delete / search, plus redo, as Spark jobs. One engine
  per executor JVM; concurrency comes from Spark parallelism alone (no "threads per worker" knob).
- **Ingest from anywhere** — a pluggable `RecordSource` seam: **RabbitMQ** (via a durable parquet
  inbox), **Kafka**, **Delta**, or [your own source](docs/tutorial/05-adopt-your-own-source.md).
  Straggler-free by design — see the [overlapping-batch feeder](docs/PARALLEL_BATCH_FEEDER.md).
- **Replicate the results** — every add runs `WITH_INFO`, emitting a change feed of affected entities
  (plus a **dead-letter** feed), so you can materialize the resolved entity graph into Delta / a
  warehouse / an online store in real time. Use the **built-in
  [entity-mart](docs/tutorial/07-entity-mart-replication.md)** (resolved entities → Delta tables, local
  or Databricks Unity Catalog) or [adapt your own](docs/tutorial/06-adapt-your-own-replication.md).
- Opt-in **`getStats`** engine self-instrumentation to the driver log (`SZ_STATS` prefix).

## What you can build with it

The engine core is identical across all three — only the source and sink adapters change:

| Environment | Ingest | Where results land | What it looks like |
|---|---|---|---|
| **On-prem (POSIX)** | RabbitMQ → parquet inbox → engine | Co-located SQL repo (PostgreSQL / MSSQL / MySQL) + Delta/warehouse | [RabbitMQ setup](docs/tutorial/03-rabbitmq-setup.md) · [on-prem tutorial](docs/tutorials/spark-onprem.md) |
| **Streaming / lakehouse** | Kafka (or Delta) → engine | Object storage, Delta tables | [Kafka setup](docs/tutorial/04-kafka-setup.md) · [AWS EMR tutorial](docs/tutorials/aws-emr.md) |
| **Databricks-native** | Auto Loader / Kafka / Delta | Unity Catalog, online/synced tables | [Databricks reference](docs/DATABRICKS.md) · [Databricks tutorial](docs/tutorials/databricks.md) |

## Start here

**New to sz_spark?** → read the **[Guides](docs/tutorial/README.md)** — a short, progressive series:
[architecture](docs/tutorial/01-architecture.md) → [getting started](docs/tutorial/02-getting-started.md)
→ the [RabbitMQ](docs/tutorial/03-rabbitmq-setup.md) and [Kafka](docs/tutorial/04-kafka-setup.md) setups
→ [adopting your own source](docs/tutorial/05-adopt-your-own-source.md) and
[replication](docs/tutorial/06-adapt-your-own-replication.md). Then use the build + ops quick start below.

---

## Build

Two artifacts: the **FAT jar** (needs a licensed Senzing dist; built once) and the **Docker image**
(wraps the jar with the exact Spark runtime + PostgreSQL client libraries it needs).

### 1. Build the FAT jar

Requires JDK 17/21, sbt, and a **local licensed Senzing dist** (`senzingsdk-runtime`). The SDK jar and
native libs are **not** on Maven Central and **must not** be redistributed — point `SENZING_DIR` at your
install (default `/opt/senzing`). Full detail in [`docs/BUILD.md`](docs/BUILD.md).

```bash
export SENZING_DIR=/opt/senzing        # your local licensed install
sbt stageNatives                       # stage native libs/data/resources/config (gitignored)
sbt -J-Xmx8g assembly                  # -> target/scala-2.13/sz-spark-assembly.jar  (~265 MB)
```

### 2. Build the Docker image

The image build needs the jar in its context. Copy it to the repo root, then build:

```bash
cp target/scala-2.13/sz-spark-assembly.jar ./sz-spark-assembly.jar
docker build -t sz_spark:latest .
```

The [`Dockerfile`](Dockerfile) rebases the official Spark 4.0.1 image onto Ubuntu 24.04 (the Senzing
natives need glibc ≥ 2.38) and installs the PostgreSQL client closure the bundled plugin dlopens
(the `SENZ0087` / chunked-rows fixes). The jar self-extracts its native payload at runtime — the image
carries **no** `/opt/senzing` install. The jar is gitignored and never published; it only enters the
build context locally.

---

## Configure

The engine is configured entirely from **`SENZING_ENGINE_CONFIGURATION_JSON`** (an env var / secret —
never hardcoded). At minimum it carries the SQL connection:

```bash
export SENZING_ENGINE_CONFIGURATION_JSON='{
  "PIPELINE":{"CONFIGPATH":"...","RESOURCEPATH":"...","SUPPORTPATH":"..."},
  "SQL":{"CONNECTION":"postgresql://USER:PASS@DBHOST:5432/senzing"}}'
export PGSSLMODE=require        # for managed/cloud Postgres; "disable" only for a local no-SSL dev PG
```

In FAT-jar mode the runtime rewrites the three `PIPELINE` paths to the self-extracted trees, so their
values are placeholders. Generate the JSON **attribute mapping** for your records with the Senzing-MCP
`mapping_workflow` — a wrong mapping silently degrades resolution rather than erroring.

**Initialize the database schema first (run once).** `InitJob` applies the schema DDL and registers the
default config plus your data sources. It is a standalone JVM step — never run it on an executor, and
never inside a data job. `dataSources=` here **registers** the data source codes in the config (this is
distinct from the record-level `DATA_SOURCE` in the input contract below).

```bash
docker run --rm \
  -e SENZING_ENGINE_CONFIGURATION_JSON \
  -e PGSSLMODE \
  sz_spark:latest \
  java -cp /opt/sz/sz-spark-assembly.jar com.senzing.spark.jobs.InitJob \
    dialect=postgresql \
    db='jdbc:postgresql://DBHOST:5432/senzing?sslmode=require&user=USER&password=PASS' \
    dataSources=CUSTOMERS,WATCHLIST
```

`db=<jdbcUrl>` is **required** for postgresql/mysql/mssql (omit only for SQLite, which auto-creates).
Without it the schema step is silently skipped and later jobs fail as "engine not initialized".

---

## Run

Every job is a `spark-submit` from the one jar. Because `libSz` dlopens plugins **by soname**,
`LD_LIBRARY_PATH` must point at the jar's self-extract `lib/` dir **at JVM launch** — a per-jar path
`$SENZING_EXTRACT_DIR/sz-spark-<sha256-of-jar>/lib`. The snippet below computes it; reuse it in each
example.

```bash
# Common docker-run scaffold. Mount your data, pass the engine config, set LD_LIBRARY_PATH.
ECJ="$SENZING_ENGINE_CONFIGURATION_JSON"
run() {   # run <spark-submit-and-jar-args...>
  docker run --rm \
    -e SENZING_ENGINE_CONFIGURATION_JSON="$ECJ" -e PGSSLMODE=require \
    -v "$PWD/data:/data" \
    sz_spark:latest bash -lc '
      JAR=/opt/sz/sz-spark-assembly.jar
      SHA=$(sha256sum "$JAR" | cut -d" " -f1)
      export LD_LIBRARY_PATH="$SENZING_EXTRACT_DIR/sz-spark-$SHA/lib"
      '"$*"
}
```

### Add / update (and delete)

`addRecord` per record with info; writes deduped **affected entity IDs** + an error frame. `DeleteJob`
is identical with `--class ...DeleteJob`.

```bash
run 'spark-submit --class com.senzing.spark.jobs.AddUpdateJob \
  --conf spark.speculation=false \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memory=2g \
  --conf spark.executor.memoryOverhead=8g \
  "$JAR" \
  input=/data/customers.jsonl \
  output=/data/out/affected errors=/data/out/errors staging=/data/out/staging \
  partitions=64 runId=load-1'
```

There is **no `dataSource=` argument** — each record supplies its own `DATA_SOURCE` (see Input contract).
`memoryOverhead ≈ (4 + cores) GB` because the engine is native/off-heap (see
[`docs/RUNBOOK.md`](docs/RUNBOOK.md)).

### Search

`searchByAttributes` per request; writes **request paired with results** + an error frame. Read-only.

```bash
run 'spark-submit --class com.senzing.spark.jobs.SearchJob \
  --conf spark.executor.cores=4 --conf spark.executor.memoryOverhead=8g \
  "$JAR" \
  input=/data/search-requests.jsonl \
  output=/data/out/search-results errors=/data/out/search-errors staging=/data/out/search-staging \
  partitions=64 runId=search-1'
```

### Redo (scheduled)

`RedoJob` drains the engine's redo queue and processes it in parallel. Run it on a **schedule** (the
queue refills; `getRedoRecord()==null` is **not** "done"); run **one** instance at a time.

```bash
run 'spark-submit --class com.senzing.spark.jobs.RedoJob \
  --conf spark.executor.cores=4 --conf spark.executor.memoryOverhead=8g \
  "$JAR" \
  output=/data/out/redo-affected errors=/data/out/redo-errors staging=/data/out/redo-staging \
  partitions=64 redoBatch=100000 runId=redo-1'
```

### Streaming ingest (queue → parquet inbox → feeder)

Two decoupled stages joined by a durable **parquet inbox** (full design in
[`docs/RABBITMQ_INGEST.md`](docs/RABBITMQ_INGEST.md)):

**Stage 1 — the drainer** (`glue.MqToParquet`): a plain-JVM competing consumer that reads records off a
message queue and writes parquet shards, acking **only after** each shard is persisted (write-ahead, so
records are never dropped). Run it next to the queue.

```bash
run 'spark-submit --class com.senzing.spark.glue.MqToParquet "$JAR" \
  amqpUrl=amqp://USER:PASS@MQHOST:5672/%2F queue=YOUR_QUEUE \
  inbox=/data/io/inbox prefetch=5000 shardRecords=5000 emptyMs=30000'
```

**Stage 2 — the feeder** (`glue.ParquetStreamFeeder`): a long-running Structured Streaming query that
reads the inbox and `addRecord`s each micro-batch, with durable **dead-letter** and change-feed sinks.

```bash
run 'spark-submit --class com.senzing.spark.glue.ParquetStreamFeeder \
  --conf spark.speculation=false \
  --conf spark.executor.cores=4 --conf spark.executor.memoryOverhead=8g \
  "$JAR" \
  inbox=/data/io/inbox checkpoint=/data/io/checkpoint archive=/data/io/archive \
  staging=/data/io/staging \
  deadLetter=/data/io/deadletter output=/data/io/output \
  maxFilesPerTrigger=200 trigger=default runId=stream-1'
```

Failed records are written to `deadLetter=` (not dropped, not silently defaulted) and can be replayed
with `glue.DeadLetterReprocess`. See [`docs/DEAD_LETTER.md`](docs/DEAD_LETTER.md).

---

## Input contract (required per record)

Each input record is standard Senzing **mapped JSON** (one JSON object per line, JSONL). Every record
**must carry its own** `DATA_SOURCE` **and** `RECORD_ID` in the JSON body — both are read from the
record itself (there is no per-job `dataSource=` argument). A record missing **either** key is routed
to the **dead-letter** sink as `BAD_INPUT` **without calling the engine** — it is never loaded and never
silently defaulted.

```json
{"DATA_SOURCE":"CUSTOMERS","RECORD_ID":"1001","PRIMARY_NAME_FULL":"Jane Doe", "...": "..."}
```

The `DATA_SOURCE` value must have been **registered** first via `InitJob dataSources=…`; an unregistered
source is a config error routed to the error frame. Use the Senzing-MCP `mapping_workflow` for the
attribute mapping.

---

## Monitoring

- **`SZ_STATS` sampler** — set `--conf spark.plugins=com.senzing.spark.diag.StatsPlugin` to emit the
  engine's reset-on-read `getStats()` periodically to the driver log (one sampler per executor JVM). See
  [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md) §Monitoring.
- Watch per-task progress lines (interval + cumulative rate) and `LONG_RECORD` warnings.
- `countRedoRecords()` is a coarse health gauge only — **never** a loop condition.

Results: the affected-entity frame is a change-**notification** feed (IDs only) — re-query
`getEntity` / `getEntityByRecordId` per affected ID for settled content (see
[`docs/RUNBOOK.md`](docs/RUNBOOK.md) "Reading results").

---

## Documentation

**New here? Start with the [Guides](docs/tutorial/README.md)** — a progressive series covering the
architecture, getting started, the [RabbitMQ](docs/tutorial/03-rabbitmq-setup.md) and
[Kafka](docs/tutorial/04-kafka-setup.md) setups, and how to
[adopt your own source](docs/tutorial/05-adopt-your-own-source.md) and
[replication](docs/tutorial/06-adapt-your-own-replication.md).

Reference docs:

- Architecture (how Senzing runs on Spark): [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- Runnable `spark-submit` examples: [`docs/EXAMPLES.md`](docs/EXAMPLES.md)
- Performance & sizing (scaling to billions of records): [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md)
- Streaming ingest design (queue → inbox → feeder): [`docs/RABBITMQ_INGEST.md`](docs/RABBITMQ_INGEST.md)
- Dead-letter capture & reprocess: [`docs/DEAD_LETTER.md`](docs/DEAD_LETTER.md)
- Core vs glue vs diag job layering: [`docs/JOB_LAYERING.md`](docs/JOB_LAYERING.md)
- Troubleshooting (Spark-specific failure modes): [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md)
- Ops runbook: [`docs/RUNBOOK.md`](docs/RUNBOOK.md)
- Deployment tutorials: [on-prem Spark](docs/tutorials/spark-onprem.md) (✅ **validated on-prem at
  100M+ records**) · [AWS EMR](docs/tutorials/aws-emr.md) · [Databricks](docs/tutorials/databricks.md)
  (⚠️ EMR/Databricks **not yet tested directly**; the same application is validated on-prem)
- Databricks reference: [`docs/DATABRICKS.md`](docs/DATABRICKS.md)
- Design (implementation): [`docs/DESIGN.md`](docs/DESIGN.md)

### Developing

Contributors building/testing without Docker: `sbt compile`, `sbt test` (fast plumbing suite — unit +
spark-local with a fake engine), `sbt scalafmtAll`, and `./scripts/it-local.sh` (real-engine
integration on SQLite). Full workflow in [`docs/BUILD.md`](docs/BUILD.md). SQLite is dev-only (no
concurrent writes) — use a co-located PostgreSQL for anything beyond a single-process smoke.
