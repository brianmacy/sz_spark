# sz_spark — Databricks deployment

A deploy guide; see `docs/RUNBOOK.md` for the binding preconditions, `docs/PERFORMANCE.md` for sizing,
and `docs/TROUBLESHOOTING.md` for failure modes.

> ⚠️ **Not yet validated on a live Databricks cluster** (NEXT_STEPS #9). The required-conf and
> preconditions are derived from the verified design and the SDK's native-loading facts; the concrete
> cluster steps below (init scripts, cluster libraries, Volumes paths) are **intended configuration**
> to walk through and confirm on a real DBR 14+ cluster, then mark validated. Treat them as a starting
> checklist, not a tested recipe.

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

## FAT jar location (Unity Catalog Volumes / DBFS)

Put the locally-built jar on storage the cluster can read:

- **Unity Catalog Volume** (preferred): `/Volumes/<catalog>/<schema>/<volume>/sz-spark-assembly.jar`.
- **DBFS** (legacy / non-UC workspaces): `dbfs:/FileStore/jars/sz-spark-assembly.jar`.

Reference that path as the task JAR. The jar is your own build artifact — it must live in **your**
workspace storage and is never published to a public location.

## Cluster libraries

Attach the FAT jar as a **cluster (or job) library** pointing at the Volume/DBFS path above so it is on
the classpath of the driver and every executor. Because Spark is `provided`, do **not** also attach
Spark — the runtime supplies it. Pin the cluster to a DBR whose Spark is 4.0 / Scala 2.13 / JDK 17–21.

## Init scripts (native libs on executors)

The mandatory launch env var cannot be set after the JVM starts, so set it in **cluster Spark conf**
(below), not an init script. Use a **cluster-scoped init script** only for things that must exist on
the node image before the JVM starts:

- Install the **DB client closure** if the DBR image lacks it for your dialect (PostgreSQL `libpq.so.5`
  + chain; MSSQL ODBC driver; MySQL `libmysqlclient`). Alternatively use the air-gapped FAT-jar variant
  that stages the client into `lib/`.
- Optionally pre-create / pin `SENZING_EXTRACT_DIR` on a non-tmpfs path with ≥ ~1 GB free, so the
  per-node self-extraction has a stable, known location for the `LD_LIBRARY_PATH` value.

Confirm the worker image's **glibc ≥ 2.34** (the verified floor; `GlibcCheck` enforces it at startup).

## Secrets & Unity Catalog (database credentials)

`SENZING_ENGINE_CONFIGURATION_JSON` carries the DB connection string — treat it as a secret, never
hardcode it.

- Store it in a **Databricks secret scope** (`databricks secrets put`), or a UC-governed secret, and
  inject it as a cluster **environment variable** referencing the secret:
  `SENZING_ENGINE_CONFIGURATION_JSON={{secrets/<scope>/<key>}}`.
- For Aurora/Azure with token auth, prefer instance-profile / Managed Identity over embedding a
  password; co-locate the DB with the cluster (same region/AZ/VPC) for latency — see
  `docs/PERFORMANCE.md`.

## Databricks Workflows: notebook vs. JAR task

- **JAR tasks** are the primary form: one task per job (`InitJob`, `AddUpdateJob`, `DeleteJob`,
  `SearchJob`, `RedoJob`) with the main class and `key=value` args (see `docs/EXAMPLES.md`).
- **Init must be its own task**, run once, ordered before the loaders — never inline in a loader task
  (two `SzEnvironment` instances in one process crash the engine).
- **Notebooks** are fine for inspection / `ShowOutput`-style exploration, but the engine still follows
  one-env-per-JVM — don't build a second environment in a notebook attached to a job cluster.
- Schedule `RedoJob` as a **recurring** Workflow (or a hard-ordered phase after loaders); a single
  instance at a time.

## Autoscaling caveats

- The engine builds **once per executor JVM and is destroyed only at JVM shutdown**. Autoscaling that
  adds executors is fine (each new JVM builds its own engine). Aggressive **scale-down mid-job** kills
  executor JVMs holding in-flight, uncancellable verbs and (for `RedoJob`) can drop the in-flight redo
  batch — prefer fixed-size clusters for large loads, or conservative scale-down.
- Each engine thread holds a **DB connection for its lifetime**; autoscaling up raises the total
  connection count — size the database `max_connections` for the **max** executor count, not the
  initial one (see `docs/PERFORMANCE.md`).
- Off-heap `memoryOverhead` must be correct on **every** node size the autoscaler may pick; a smaller
  node type with the same cores still needs `(4 + cores) GB` overhead.
