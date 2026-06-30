# Tutorial: sz_spark on AWS EMR

> ⚠️ **DRAFT — untested.** Not yet validated on a live EMR cluster. Verify every step (especially the
> GLIBC/AMI and bootstrap package names) before relying on it. Cross-check Senzing specifics against
> the **Senzing-MCP** and your installed dist.

Runs the sz_spark jobs on EMR against **Aurora/RDS PostgreSQL**.

## Critical EMR prerequisite: AMI / GLIBC

The Senzing native libs require **GLIBC ≥ 2.34**. Use **EMR 7.x (Amazon Linux 2023, glibc 2.34+)** —
**not** EMR 6.x (Amazon Linux 2, glibc 2.26), which will fail `GlibcCheck` at startup. Confirm the
exact glibc on your chosen EMR release before building the cluster.

## Prerequisites

- Licensed Senzing dist on the build host; FAT jar built locally (never published — no SDK
  redistribution). Build per `docs/tutorials/spark-onprem.md` §1 and note `JAR_SHA`.
- An S3 bucket for the jar + bootstrap script (your own private bucket).
- **Aurora PostgreSQL** (recommended: IO-Optimized), same region/AZ as the cluster, in private
  subnets. Security group allowing `5432/tcp` from the EMR nodes. `rds.force_ssl=1` ⇒ `PGSSLMODE=require`.

## 1. Upload the jar

```bash
aws s3 cp target/scala-2.13/sz-spark-assembly.jar s3://YOUR_BUCKET/sz-spark/sz-spark-assembly.jar
```

## 2. Bootstrap action — install the DB-client + crypto libs on every node

`libSz` `dlopen`s the PostgreSQL plugin (`libpq.so.5`) by soname and NEEDs OpenSSL 3. Put this script
in S3 and register it as an EMR **bootstrap action** (verify package names for your AL2023 release):

```bash
#!/usr/bin/env bash
set -euo pipefail
sudo dnf install -y libpq openssl    # provides libpq.so.5 and libssl.so.3 ; verify names on AL2023
```

```bash
aws s3 cp bootstrap-senzing.sh s3://YOUR_BUCKET/sz-spark/bootstrap-senzing.sh
```

## 3. Aurora tuning + schema/config init

Apply Senzing-relevant Aurora tuning (per Senzing-MCP "AWS deployment considerations"):
`synchronous_commit=off` (during bulk loads), `max_connections >= cores_per_executor ×
(num_executors+1) + overhead`, custom parameter group. Apply the schema DDL and register config with a
**one-time `InitJob`** — run it from a bastion/admin host in the same VPC (so it can reach Aurora), or
as a pre-load EMR step. Example from a bastion with the dist installed:

```bash
# Aurora uses its OWN connection scheme + plugin (libaurorapostgresqlplugin); plain RDS PostgreSQL uses
# postgresql://. CONFIGPATH = the Senzing config dir (/etc/opt/senzing on a standard install, or your
# sz_create_project etc/). RESOURCEPATH/SUPPORTPATH are distinct trees.
export SENZING_ENGINE_CONFIGURATION_JSON='{"PIPELINE":{"CONFIGPATH":"/etc/opt/senzing","RESOURCEPATH":"/opt/senzing/er/resources","SUPPORTPATH":"/opt/senzing/data"},"SQL":{"CONNECTION":"aurorapostgresql://USER:PASS@AURORA_ENDPOINT:5432/G2"}}'
export PGSSLMODE=require LD_LIBRARY_PATH=/opt/senzing/er/lib
java -cp sz-spark-assembly.jar com.senzing.spark.jobs.InitJob \
  dialect=postgresql db="jdbc:postgresql://AURORA_ENDPOINT:5432/G2?user=USER&password=PASS&sslmode=require" \
  dataSources=CUSTOMERS
```

## 4. Launch the cluster with the right Spark config

Pass engine env + native/off-heap memory via EMR **Configurations** (`spark-defaults`) and the
bootstrap action. Key settings (engine is native/off-heap; dlopen-by-soname needs launch
`LD_LIBRARY_PATH`):

```json
[
  { "Classification": "spark-defaults",
    "Properties": {
      "spark.executor.cores": "4",
      "spark.executor.memory": "2g",
      "spark.executor.memoryOverhead": "8g",
      "spark.speculation": "false",
      "spark.executorEnv.SENZING_EXTRACT_DIR": "/var/tmp",
      "spark.executorEnv.LD_LIBRARY_PATH": "/var/tmp/sz-spark-JAR_SHA/lib",
      "spark.executorEnv.PGSSLMODE": "require",
      "spark.executorEnv.SENZING_ENGINE_CONFIGURATION_JSON": "{...same ECJ as above...}",
      "spark.yarn.appMasterEnv.SENZING_ENGINE_CONFIGURATION_JSON": "{...same ECJ...}"
    }
  }
]
```

Replace `JAR_SHA` with the value from the build. Pick **memory-optimized instances** (r-series) sized
so each node's RAM covers `spark.executor.memory + memoryOverhead` per executor (≈ 4 GB engine + 1 GB
per core, off-heap). Keep all Senzing compute + Aurora in the **same AZ**.

## 5. Submit jobs as EMR steps

```bash
aws emr add-steps --cluster-id j-XXXX --steps '[{
  "Type":"CUSTOM_JAR","Jar":"command-runner.jar","Name":"sz-add-update",
  "Args":["spark-submit","--deploy-mode","cluster",
          "--class","com.senzing.spark.jobs.AddUpdateJob",
          "s3://YOUR_BUCKET/sz-spark/sz-spark-assembly.jar",
          "input=s3://YOUR_BUCKET/data/customers.jsonl",
          "output=s3://YOUR_BUCKET/out/affected","errors=s3://YOUR_BUCKET/out/errors",
          "staging=s3://YOUR_BUCKET/staging/addupdate","dataSource=CUSTOMERS","partitions=64"]
}]'
```

Swap `--class` for `DeleteJob` / `SearchJob`. Run **`RedoJob` on a schedule** (EventBridge → add-steps,
or Airflow/MWAA) — the redo queue refills, so it's recurring, one instance at a time.

## Gotchas

- **EMR release / GLIBC** is the #1 trap — use EMR 7.x (AL2023). Verify glibc ≥ 2.34.
- The launch `spark.executorEnv.LD_LIBRARY_PATH` (sha extract dir) is mandatory; `$ORIGIN` doesn't
  cover dlopen-by-soname. If EMR can't set it, the deploy fails by design.
- `memoryOverhead` (not heap) carries the native engine — size `4 GB + 1 GB/core`.
- Aurora Serverless v2 underperforms for Senzing — prefer provisioned IO-Optimized.
- Bastion `InitJob` needs network to Aurora; never run init inside a Spark job (two envs/process crash).
- Validate one core node first: SSH in and run `com.senzing.spark.diag.SelfCheck` with the extract-dir
  `LD_LIBRARY_PATH` before launching a full load.

See `docs/RUNBOOK.md` and `docs/DESIGN.md`.
