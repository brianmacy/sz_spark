# Threading model and engine lifecycle

## How many Senzing engines should run, and where?

**One `SzEnvironment`/`SzEngine` per executor JVM** — not per task, partition, or record. The engine
must be created once per process and shared across threads (it is thread-safe). Creating a
factory/environment per call or per thread leaks resources and throws conflict errors.

In Spark, the *process* is the **executor JVM**, so the engine is a JVM-singleton (`SzEngineProvider`).

## Refcount to zero — should I destroy the engine then?

**No.** Create the engine **once** and **destroy it only at JVM shutdown** (one ordered shutdown hook:
`env.destroy()` awaited, then native-dir cleanup). Do **not** destroy-then-rebuild within a living JVM:

- Databricks reuses executor JVMs across stages, so a refcount-to-zero-then-rebuild churns
  build/destroy needlessly (build is expensive; teardown is costly).
- Only one `SzCoreEnvironment` may be active per process — stage overlap or a stray driver/notebook env
  makes rebuild fragile (`build()` throws if one is already active).

`acquire()`/`release()` therefore maintain a **liveness counter only** (for progress/stats cadence) —
they never destroy at zero. (The SDK *does* permit recreate-after-destroy; we just don't use it
mid-life.)

## Why single-threaded per task? Doesn't Senzing want many threads?

Senzing's generic "use a thread pool" advice targets a *single-process* loader. Here **Spark partitions
are the parallelism** — many tasks across executors hit the shared engine concurrently. Each task drives
the engine **single-threaded**; concurrency is the executor/worker count, with no "threads per worker"
knob and **no intra-task thread pool** (the one sanctioned exception is `RedoJob`'s optional in-process
futures pool, a standalone app — see `redo/redo-strategy`).

## What if another process changes the Senzing config? (live config updates)

This is the mechanism for **live config updates**: a steward/admin activates a new default config in
the DB (data sources, features, ER rules) and running Spark jobs adopt it **without a restart**.

The logic (see `ConfigDrift`):

1. **Periodically** (every ~60 s, between records) compare `getActiveConfigId()` to
   `getConfigManager().getDefaultConfigId()` — **both on `SzEnvironment`, not `SzEngine`** — and if
   they differ, `reinitialize(defaultId)`.
2. **On a config-relevant operation error**, do the same check immediately and retry the record once
   on the refreshed config (then propagate the error if it recurs).

Two concurrency rules make this safe:

- **Reinit under the write lock** so in-flight verbs (which hold the read lock) quiesce first.
- **Double-check the ids inside the lock** so only the FIRST thread reinitializes — without it,
  several workers that each saw drift would each reinit in turn, **stacking** redundant reinits. The
  periodic path is additionally CAS-throttled (~1/min) so an error storm can't stampede the lookup.

> Note: the Senzing-MCP does not explicitly certify `reinitialize()` concurrency safety even with verbs
> quiesced — confirm with Senzing before relying on it under heavy churn.
