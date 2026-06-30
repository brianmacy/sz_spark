# sz_spark — Databricks deployment

A condensed deploy guide; see `docs/RUNBOOK.md` for the binding preconditions.

## Cluster requirements

- **Runtime**: a DBR whose Spark is **4.0** / Scala **2.13** and JDK 17/21; pin the build's
  `scalaVersion` to match exactly (a 2.12↔2.13 mismatch under `provided` scope is a runtime
  `NoSuchMethodError`).
- **GLIBC ≥ 2.34** on the workers (verified floor).
- **DB-client libraries** for your dialect present on the worker image (PostgreSQL `libpq.so.5` + chain,
  etc.) — or use the air-gapped FAT-jar variant that stages them.

## Required Spark conf (cluster / job)

```
spark.executorEnv.LD_LIBRARY_PATH = <SENZING_EXTRACT_DIR>/<sha>/lib   # MANDATORY (dlopen-by-soname)
spark.speculation                 = false                            # engine calls are side effects
spark.executor.cores              = N                                # = concurrent Sz threads/executor
spark.executor.memory             = 2g                               # JVM heap (modest; records stream)
spark.executor.memoryOverhead     = (4 + N) g                        # native/off-heap: 4GB engine + 1GB/thread
```

**Memory is native/off-heap** — Senzing is C, so its RAM counts against `memoryOverhead`, not heap. Rule
of thumb: **4 GB per executor + 1 GB per core** (concurrent Sz thread). Under-sizing `memoryOverhead`
gets executors OOM-killed under load. The FAT jar is ~265 MB (not multi-GB).

`<SENZING_EXTRACT_DIR>` defaults to `/var/tmp`; the `<sha>` subdir is per-jar (the jar self-extracts
there once per node). Pin `SENZING_EXTRACT_DIR` if you want a stable path for the launch env var, or
stage natives to a known location.

Provide `SENZING_ENGINE_CONFIGURATION_JSON` and `PGSSLMODE=require` via cluster env / secrets.

## Job ordering (Databricks Workflows)

1. **Init** (run once, separate task): `InitJob` — schema DDL + default config.
2. **Loaders**: `AddUpdateJob` / `DeleteJob` (parallel by partitions).
3. **Redo**: `RedoJob` on a **schedule** (recurring), or as a hard-ordered phase after loaders for a
   batch `load → drain → read` sequence. Run a single RedoJob instance at a time.
4. **Search**: `SearchJob` as needed (read-only).

## Build & upload (local, never published)

```
export SENZING_DIR=/opt/senzing          # local licensed install
sbt stageNatives                          # stage native payload (gitignored)
sbt -J-Xmx8g assembly                     # FAT jar at target/scala-2.13/sz-spark-assembly.jar
SZ_IT=1 sbt "testOnly *FatJarIT -- -n com.senzing.spark.IntegrationTest"   # verify bundling
```

Upload the FAT jar to your own workspace/volume (DBFS/Unity Catalog volume) and reference it as the job
JAR. **Never** commit or publish it — it bundles the Senzing SDK.
