# sz_spark

A reference example for calling the **Senzing V4 SDK** from **Spark/Databricks** jobs against a
co-located SQL database (PostgreSQL/MSSQL/MySQL), packaged as a self-extracting **FAT jar**.

It demonstrates **add/update**, **delete**, and **search** (each producing an output DataFrame plus an
error DataFrame), and processes **redo** as a scheduled, parallel job. Concurrency is controlled
entirely by Spark executor/worker count — there is no "threads per worker" knob; each task drives a
shared per-executor-JVM engine single-threaded.

- Architecture (how Senzing runs on Spark): [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- Building the FAT jar: [`docs/BUILD.md`](docs/BUILD.md)
- Examples (runnable `spark-submit`): [`docs/EXAMPLES.md`](docs/EXAMPLES.md)
- Performance & sizing (scaling to billions of records): [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md)
- Troubleshooting (Spark-specific failure modes): [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md)
- Databricks deployment: [`docs/DATABRICKS.md`](docs/DATABRICKS.md)
- Design (implementation): [`docs/DESIGN.md`](docs/DESIGN.md)
- Build plan (milestones + test gates): [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md)
- Ops runbook: [`docs/RUNBOOK.md`](docs/RUNBOOK.md)
- Deployment tutorials (⚠️ **DRAFT — untested**): [on-prem Spark](docs/tutorials/spark-onprem.md) ·
  [AWS EMR](docs/tutorials/aws-emr.md) · [Databricks](docs/tutorials/databricks.md)
- Working agreement / guardrails: [`CLAUDE.md`](CLAUDE.md)

## Build

Requires JDK 17/21, sbt, and a **local licensed Senzing dist** (`senzingsdk-runtime`) — the SDK jar and
native libs are **not** on Maven Central and **must not** be redistributed. Point `SENZING_DIR` at the
install (default `/opt/senzing`).

```bash
sbt compile                 # compile against the local SDK jar
sbt test                    # full non-integration suite (unit + spark-local, fake engine)
sbt scalafmtAll             # format
sbt stageNatives            # stage native libs/data/resources/config (local, gitignored)  [M9]
sbt -J-Xmx8g assembly       # FAT jar (big heap for the native payload)                     [M9]
./scripts/it-local.sh       # REAL-engine integration tests (SQLite, single-process dev)     [M15]
```

`sbt test` is the fast **plumbing** suite (no engine — unit + spark-local with a fake engine).
`./scripts/it-local.sh` is the suite that actually exercises **entity resolution**: it stands up a
real engine on SQLite, runs `InitJob`, then `EngineIT` (real `addRecord`/`searchByAttributes`/redo
through the Spark jobs). Use a co-located PostgreSQL for anything beyond a single-process smoke.

The FAT jar, SDK jar, native libs, and `.deb`s are gitignored and never published.
