# Why initialization is a separate step from the Spark job

## Can the Spark job create the schema and register the default config?

**No.** Schema DDL and default-config registration are **one-time administrative tasks** and must NOT
run inside the Spark job.

The reason is a hard Senzing constraint: two `SzEnvironment`/factory instances in one process corrupt
the C library's global state and crash (typically minutes in). An init-then-load script that creates
one environment to register config and another to load records will crash.

## What does the init step do?

A separate, run-once step (its own job/script, not the Spark loader):

1. Apply the Senzing schema DDL to the co-located SQL database (PostgreSQL/MSSQL/MySQL).
   The schema files ship under `/opt/senzing/er/resources/schema/`.
2. Create exactly **one** environment, register the default config (data sources, features, rules),
   set it as default.
3. Destroy that environment and exit.

The Spark job then assumes the schema and a default config already exist.

## Where does the engine configuration come from?

From `SENZING_ENGINE_CONFIGURATION_JSON` (environment variable / secret) — never hardcoded, since it
carries the database connection string. For cloud-managed Postgres, set `PGSSLMODE=require` before the
environment is built.

## Can the driver pass the engine config explicitly instead?

Yes (optional). The engine-facing cores take `engineConfig: Option[String] = None`
(`AddCore`/`DeleteCore`/`SearchCore`.run, `RedoCore.drain`/`process`, `GetCore.run`,
`DepartedVerify.run`, `EngineWorker.factory`). The value is captured in the task closure, so it reaches
the executors with each task (fine for a ~25 kB config; no Spark conf or system properties involved),
and `SzEngineProvider.acquire(engineConfig)` prefers it over `SENZING_ENGINE_CONFIGURATION_JSON`.
`None` (the default) keeps today's behaviour: the env var. Because there is one environment per JVM,
the first config seen is pinned (`EngineConfigPin`); a task that later brings a DIFFERENT config fails
loudly instead of silently running against the wrong repository. `InitJob` still reads the env var.

## What about config baked into the FAT jar?

Customized config *templates/resources* baked into the jar (see the build/config-overrides FAQ) are
read by the engine at runtime, but they do **not** re-register the default config in the database. A
change to the *registered* config (data sources, features, ER rules) must also be applied by this
one-time init step.
