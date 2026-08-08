# 02 · Getting started

Build the self-contained jar, resolve six records into three entities, confirm it worked. ~15 minutes.

```mermaid
flowchart LR
  build[Build the FAT jar] --> init[Init the repository schema] --> load[Load records] --> verify[Verify entities]
```

## 1. Build the jar

The jar carries the Senzing native libs, models, and schema DDL — nothing is installed on the cluster.
Point `SENZING_DIR` at a licensed Senzing SDK and build:

```bash
SENZING_DIR=/opt/senzing sbt stageNatives assembly
# → target/scala-2.13/sz-spark-assembly.jar
```

> ⚠ **Engine-build parity.** Whatever engine is in `SENZING_DIR` is baked into the jar. If you're
> joining an existing fleet, build against *that* fleet's engine and verify the `apiVersion` matches —
> see [`BUILD_AGAINST_FLEET_ENGINE.md`](../BUILD_AGAINST_FLEET_ENGINE.md).

## 2. Point at a repository

The engine needs a datastore. Set the standard Senzing engine config JSON (here, PostgreSQL):

```bash
export SENZING_ENGINE_CONFIGURATION_JSON='{"PIPELINE":{...},"SQL":{"CONNECTION":"postgresql://user:pass@host:5432:g2"}}'
```

Create the schema and register a default config **once** with `InitJob` (a one-time admin job, run off
the cluster):

```bash
spark-submit --class com.senzing.spark.jobs.InitJob \
  target/scala-2.13/sz-spark-assembly.jar db="$JDBC_URL"
```

## 3. Load records

Start with `local[*]` and a tiny input. The simplest loader reads a DataFrame of records and adds
them; the production loader is the [overlapping-batch feeder](01-architecture.md#the-feeder--why-batches-dont-stall).
Records are JSON in the [Senzing mapping format](https://senzing.com) — each carries a `DATA_SOURCE`
and `RECORD_ID`.

```bash
spark-submit --master 'local[*]' --class com.senzing.spark.jobs.AddUpdateJob \
  target/scala-2.13/sz-spark-assembly.jar input=/path/to/records.jsonl
```

The full, copy-pasteable invocation for every job is in [`EXAMPLES.md`](../EXAMPLES.md);
operating them on a real cluster is [`RUNBOOK.md`](../RUNBOOK.md).

## 4. Verify

`SelfCheck` builds the engine, adds a record, retrieves it, deletes it — a one-shot proof the whole
native stack loaded and the repository is reachable:

```bash
spark-submit --master 'local[*]' --class com.senzing.spark.diag.SelfCheck \
  target/scala-2.13/sz-spark-assembly.jar
```

A clean run means the jar self-extracted its natives, connected to the repository, and resolved a
record end-to-end. You're ready to wire in a real source.

## Where next

- On-prem, records on a message queue → **[03 · RabbitMQ setup](03-rabbitmq-setup.md)**
- Lakehouse / Databricks, records in a stream or table → **[04 · Kafka setup](04-kafka-setup.md)**
- Something else entirely → **[05 · Adopt your own source](05-adopt-your-own-source.md)**
