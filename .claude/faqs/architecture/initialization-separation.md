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

## What about config baked into the FAT jar?

Customized config *templates/resources* baked into the jar (see the build/config-overrides FAQ) are
read by the engine at runtime, but they do **not** re-register the default config in the database. A
change to the *registered* config (data sources, features, ER rules) must also be applied by this
one-time init step.
