# Testing: plumbing suite vs real-engine integration

## `sbt test` does NOT use a real engine

The default `sbt test` (~77 tests) is the **fast plumbing suite**: pure logic, Mockito mocks of
`SzEnvironment`/`SzConfigManager`, and spark-local tests with an **injected fake verb** function. No
`SzCoreEnvironment.build()`, no native libs, no DB, **no entity resolution**. It proves the wiring,
error taxonomy, lifecycle, and Spark single-pass/two-sink plumbing — and it runs in CI without the SDK.

**Plumbing-green is necessary but does not count as ER coverage.** Real entity resolution is only
exercised by the integration suite.

## Real-engine integration: `./scripts/it-local.sh`

Runs `EngineIT` (and the `EngineSmoke` runner) against a **real engine** on SQLite (single-process dev
only — never production/concurrent). The script:

1. copies `G2C.db.template` → a fresh SQLite DB (schema only; `getEngine()` throws `SENZ7220` until a
   config is registered),
2. sets `LD_LIBRARY_PATH=$SENZING_DIR/er/lib` and a SQLite `SENZING_ENGINE_CONFIGURATION_JSON`,
3. runs `InitJob dialect=sqlite dataSources=TEST` (stock config has **no** app data sources — an
   unregistered source makes `addRecord` throw `SzUnknownDataSourceException`),
4. runs `EngineIT` with `SZ_IT=1` and `-Dspark.master=local[2]`, overriding the default
   `IntegrationTest` tag exclusion.

Verified: two "Jane Doe" records resolve to one entity; search finds it; redo drains. The same flow
works using **only the staged FAT-jar payload** (`LD_LIBRARY_PATH` + PIPELINE paths under
`src/main/resources/native/...`), proving the bundle is self-sufficient.

## Gating

`EngineIT`/`FatJarIT` are tagged `com.senzing.spark.IntegrationTest`; build.sbt excludes that tag from
`sbt test` (`-l`). Run them explicitly with `-n`. `FatJarIT` additionally needs the built FAT jar.

## Proven against a real engine (closed gaps)

- **Slim-image self-extraction + dlopen-by-soname** (the #1 risk): the FAT jar runs
  `com.senzing.spark.diag.SelfCheck` on a stock `eclipse-temurin:21-jre` container with **no
  `/opt/senzing`** (only `apt install libssl3 libsqlite3-0`), self-extracts to
  `$SENZING_EXTRACT_DIR/sz-spark-<sha>`, ordered-`System.load`s the siblings, dlopens the SQLite plugin
  via launch `LD_LIBRARY_PATH=<extract>/lib`, and resolves two records to one entity.
- **PostgreSQL + concurrency**: `docker run postgres:16`, `InitJob dialect=postgresql db=jdbc:...`,
  then `AddUpdateJob partitions=4` under `-Dspark.master=local[4]` → 6 records → **3 entities, 0
  errors** (concurrent tasks sharing the per-JVM engine against real Postgres). Senzing PG connection
  format is `postgresql://user:pass@host:port/database` (set `PGSSLMODE=disable` for a local no-SSL PG).
- **Delete-of-absent (finalized)**: `deleteRecord` on a never-loaded id **returns normally with empty
  AFFECTED_ENTITIES — a benign no-op, NOT `SzNotFoundException`**. So the worker's `notFoundIsBenign`
  stays `false` (the branch never triggers for delete-of-absent); `EngineIT` asserts zero error rows.

## A dev-mode gotcha (fixed)

`isFatJar()` must check BOTH the marker resource AND that the code source is a jar **file** — because
`stageNatives` puts the marker under `src/main/resources/`, so in dev/test the resource exists but the
code source is a classes directory. Without the file check, the engine wrongly takes the extract path
and hashes a directory. (Integration testing caught this.)

## Still not exercised here

True multi-**JVM**/multi-node cluster execution (local[N] is multi-thread in one JVM), and the MSSQL/
MySQL dialects.
