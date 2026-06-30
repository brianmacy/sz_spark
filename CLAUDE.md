# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status: greenfield

As of this writing the repo contains only `LICENSE` (Apache 2.0) and `.gitignore`. There is
**no source code, build file, or test yet**. This document captures the intended architecture and the
hard Senzing/Spark constraints so the first implementation does not paint itself into a corner. Treat
the architecture and "Hard constraints" sections as design contract; the build commands describe what
*will* apply once `build.sbt` exists.

> Rust was considered as a shared core (one FFI surface feeding the JVM via JNI and Python via PyO3) —
> rejected as too complex for a reference example. The chosen stack is **Scala + sbt** (decided
> 2026-06-30): the Senzing Java SDK already wraps the native engine via JNI, so no custom FFI layer is
> needed. (`.gitignore` was originally a Rust template; it has been replaced with a JVM/sbt one.)

## What this is

An example integration showing how to call the **Senzing SDK** from **Spark/Databricks** jobs against
a co-located SQL database — the deployment pattern Senzing's largest commercial customers use. Goals:

1. A self-extracting **FAT jar** for easy deployment (Senzing Java SDK jar + native libs + data +
   resources bundled, extracted on the executor at startup).
2. **Single-threaded processing per Spark task** — concurrency is controlled entirely by executor/
   worker count, with no "threads per worker" knob.
3. Demonstrate **add/update**, **delete**, and **search**, each producing an output DataFrame:
   add/update/delete → deduped affected entity IDs per task; search → request paired with results.
4. Process **redo** (see "Redo strategy" — the least-settled design area).

## Senzing SDK guardrails (MANDATORY — read before writing any Senzing code)

A Senzing MCP server (`Senzing-MCP`) is connected and is the **exclusive source of truth** for all
Senzing facts. LLM training data about Senzing is frequently wrong. Before writing/reviewing Senzing
code: get exact **V4** signatures from `get_sdk_reference`/`generate_scaffold`; generate JSON mappings
via `mapping_workflow`; check `search_docs` `category: anti_patterns` before recommending init/
threading/redo/deploy approaches. This is V4 (`SzEnvironment`/`SzEngine`/`SzCoreEnvironment`), not V3
(`G2Engine`). Never simulate or fabricate entity-resolution results.

A second MCP server, **`sz-spark-faq`** (this repo's `.claude/faq_server.py`, registered in
`.mcp.json`), holds this project's design rationale and how-to guidance as searchable Q&A. Consult it
(`search_faqs`, `get_faq`, `get_faq_categories`) before making design assumptions, planning, or
starting a build/deploy cycle. Division of labor: CLAUDE.md = coding rules/conventions; `sz-spark-faq`
= design rationale and operational knowledge; `Senzing-MCP` = authoritative Senzing API facts.

The concrete `SzCoreEnvironment` builder shape and verb signatures live in `sz-spark-faq`
(`sdk/senzing-mcp-and-api`); verify any exact signature against `Senzing-MCP`, not memory.

### The Java SDK and native libs are NOT on Maven Central

They ship with `senzingsdk-runtime` (Maven Central artifacts are stale/community): SDK jar at
`/opt/senzing/er/sdk/java/sz-sdk.jar` (filename is `sz-sdk.jar`, **not** `sz-sdk-java.jar`), native
libs `/opt/senzing/er/lib`, data `/opt/senzing/data`, resources `/opt/senzing/er/resources`. The FAT
jar bundles all of these for self-extraction. → `sz-spark-faq` `build/fat-jar-and-native-extraction`.

## Build / test / run (Scala + sbt)

Target **Spark 4.0 / Scala 2.13** (Spark 4 dropped 2.12); pin `scalaVersion` to the exact Databricks
runtime. Standard sbt commands (apply once `build.sbt` exists):

```bash
sbt compile
sbt test
sbt "testOnly *FooSpec"                       # single test class
sbt "testOnly *FooSpec -- -z \"substring\""   # single test by name (ScalaTest -z)
sbt scalafmtAll
sbt stageNatives                              # stage Senzing libs/data/resources/config from a local
                                              # licensed dist into src/main/resources/native/ (gitignored)
sbt -J-Xmx8g assembly                         # FAT/uber jar (sbt-assembly; big heap for the native payload)
```

Spark deps must be `% "provided"` (Spark is on the cluster); the Senzing SDK jar
(`unmanagedJars` → `/opt/senzing/er/sdk/java/sz-sdk.jar`, not a Maven coordinate) + natives are bundled
instead. Executors need `spark.executorEnv.LD_LIBRARY_PATH=<extract>/lib` set at **launch** (see the
FAT-jar contract). The full implementation design lives in **`docs/DESIGN.md`** (proposed; has an
open-risks list to close before it's final); the dependency-ordered build sequence with per-milestone
test gates is in **`docs/IMPLEMENTATION_PLAN.md`** (M0→M16).

## Architecture — design contract (the binding decisions)

These are the decisions an implementation must honor. The **rationale and step-by-step how-to live in
`sz-spark-faq`** — search it (cited category/title below) before implementing any of these.

- **One `SzEnvironment`/`SzEngine` per executor JVM**, **created once and destroyed only at JVM
  shutdown** — never per task/partition/record, and never destroyed-then-rebuilt in a living JVM
  (Databricks reuses executor JVMs across stages). `acquire`/`release` maintain a **liveness counter
  only** (not a build/destroy refcount). Each task drives the engine **single-threaded**; parallelism
  is executor/worker count only, no intra-task thread pool. → `architecture/threading-and-engine-lifecycle`
- **Executor memory is native/off-heap:** Senzing is C, so size `spark.executor.memoryOverhead`
  (NOT heap) to **≈ 4 GB base engine + 1 GB per core** (concurrent Sz thread); under-sizing →
  OOM-killed executors. Memory scales with the same cores knob as concurrency.
  → `deployment/executor-memory-sizing`
- **Config-drift reinit:** between records, if `getActiveConfigId()` ≠
  `getConfigManager().getDefaultConfigId()` (**both on `SzEnvironment`, not `SzEngine`**), call
  `reinitialize(defaultId)` under a write lock that quiesces in-flight verbs; throttle ~1/min.
  → `architecture/threading-and-engine-lifecycle`
- **Init is a separate run-once step, never in the Spark job** — two environments in one process
  corrupt the C library and crash. Init applies schema DDL + registers the default config, then exits;
  the job assumes both exist. Engine config from `SENZING_ENGINE_CONFIGURATION_JSON` (never hardcoded);
  set `PGSSLMODE=require` for cloud Postgres. → `architecture/initialization-separation`
- **Outputs:** add/update/delete emit deduped `AFFECTED_ENTITIES` parsed from the `SZ_WITH_INFO`
  response (not a separate query); search emits request + results. → `architecture/output-dataframes`
- **Per-record error taxonomy → good-output vs. error DataFrame:** `SzBadInputException` = bad row
  (route + continue, never fail the task); `SzRetryableException` = backoff-retry then route;
  config-relevant = drift-check + retry once; other `SzException`/`Error` = fail the task loudly.
  → `architecture/error-handling`
- **Slow records:** verbs are synchronous, **uncancellable** JNI — never abort/relocate a running
  record; log `LONG_RECORD` past a threshold; per-task logging reports interval + cumulative rate.
  → `troubleshooting/slow-records`
- **FAT jar:** stage stock libs/data/resources/config → overlay `config/overrides/` → `patchelf
  --set-rpath '$ORIGIN'` (no `--force-rpath`) **only on bundled siblings lacking a RUNPATH** (never on
  `libSz.so`) → `sbt assembly`; at runtime self-extract once per node and rewrite the three **distinct**
  PIPELINE paths (`SUPPORTPATH`→data, `RESOURCEPATH`→resources, `CONFIGPATH`→config tree under
  `resources/templates/`). **`$ORIGIN` does NOT cover dlopen-by-soname** (`libszvec`/`libszzstd`/DB
  plugin), so you also need `spark.executorEnv.LD_LIBRARY_PATH=<extract>/lib` set at executor **launch**
  + ordered `System.load` of siblings before `libSz`. → `build/fat-jar-and-native-extraction`,
  `build/config-overrides`

## Hard constraints / anti-patterns

- **Redistribution: never commit or publish the FAT jar, the Senzing SDK jar, native libs, or
  `.deb`s.** This repo has no rights to redistribute the SDK. Gitignore the native staging dir and any
  `.debs/` staging dir; source them from a local licensed install at build time; build the jar
  locally; do **not** attach it to releases or have CI publish a bundled artifact. Exempt: a user's
  **own** config override files (`config/overrides/...`) are not Senzing SDK redistribution and are
  fine to commit and ship.
- **Do not sort/group input by a resolution key** (name, address, zip) — grouped records that resolve
  to the same entity cause lock contention. Partition input randomly; never repartition by a PII key.
- **No SQLite for anything real** — no concurrent writes. The co-located DB is PostgreSQL/MSSQL/MySQL;
  SQLite is dev-only, single-process.
- **One engine per executor JVM** (refcounted) — never per task/partition/record.
- **Redo loop**: loop until `getRedoRecord()` returns empty; **never** use `countRedoRecords()` as the
  loop condition (full table scan; monitoring only).
- **Don't `setupEnv`** in code paths or rely on it for `SENZING_ENGINE_CONFIGURATION_JSON`.
- Deploy **completely locally** — no runtime dependency on external Senzing services beyond the
  bundled SDK + the co-located DB.

### Redo strategy (decided)

**A scheduled, parallel `RedoJob` that uses `getRedoRecord()` as a source** — just like the other ops.
A driver-side single dequeuer pulls `getRedoRecord()` into a DataFrame, which is repartitioned and
processed in parallel (`processRedoRecord(_, SZ_WITH_INFO)` → `AFFECTED_ENTITIES`, `op=REDO`, same
sink). It is **recurring** because `getRedoRecord()==null` is not "done" (the queue refills + redo
begets redo). Never `countRedoRecords()` as a loop condition. Full design: `docs/DESIGN.md` §7 and
`sz-spark-faq` → `redo/redo-strategy`.

## Conventions

- Scala formatted with scalafmt; keep Spark deps `provided`.
- All tests pass before completion; tests fail loudly (no silent `println` failures).
- Reuse existing code before writing new; do not introduce parallel implementations.
