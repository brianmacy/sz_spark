# Project Status

**Date:** 2026-08-07
**Branch:** bem_parallel_batch_feeder — pushed; **PR #5 open** against `main`
**State:** the M0–M16 reference implementation + streaming ingest path are on `main` (PR #4 merged
2026-08-07). This branch adds the **source-agnostic overlapping-batch feeder** (Step 1, RabbitMQ path)
that kills the micro-batch straggler tail. Implemented, unit-tested (102/102), and deployed on `.142`.
Next: monitor PR #5 CI / await review.

## Current branch work (`bem_parallel_batch_feeder`)

Commits (oldest → newest):

| SHA | Summary |
|---|---|
| `05fe7af` | v1 overlapping-batch parallel feeder (had an over-decomposition regression) |
| `ad13d47` | v2 source-agnostic overlapping-batch engine (fixes v1) |
| `96b2b4b` | record-based knobs (`recordsPerBatch` / `maxUnprocessedBatches`) |
| `87f699d` | **1-partition-per-batch operating point — the fix (no mid-stream tail)** |

New assets: `glue.RecordSource` (source seam), `glue.InboxSource` (RabbitMQ dispose adapter),
`glue.OverlappingBatchEngine` (K persistent worker threads), `glue.ShardIo` (atomic sinks + claim/
dispose/reclaim, shared with `MqToParquet`), `glue.ParquetParallelFeeder` (entry point), and
`ParquetParallelFeederSpec` (8 tests incl. straggler-isolation via a filesystem-free `MemSource`).
Uncommitted in the tree: `docs/BUILD_AGAINST_FLEET_ENGINE.md` (new) + the doc/FAQ/CHANGELOG updates
made this session (see "Uncommitted state").

**Operating point (locked):** `recordsPerBatch=1000` ⇒ one partition per batch, `maxUnprocessedBatches`
≥ `spark.cores.max`. Each batch commits independently, so a huge-entity straggler parks exactly 1 of K
workers and the rest keep cycling — no mid-stream tail, only genuine end-of-input. See
`docs/PARALLEL_BATCH_FEEDER.md`.

## ★ The engine-build-parity finding (2026-08-07)

The feeder measured ~20% slower than the native Rust fleet on the same DB. After ruling out host,
executor topology, connections, network, and contention, the root cause was a **stale engine build
baked into the FAT jar**: `sbt stageNatives` pulls the engine from `$SENZING_DIR` (`/opt/senzing`),
which was `4.4.0.26151` (2026-05-31), while the fleet ran `4.4.0.DEVELOPMENT` with newer DB-round-trip
reductions. Rebuilding the jar against the fleet's engine restored parity: `.142`-Spark host CPU
44% → 55.5% ≈ `.141`-Rust 57%; total system add 1562 → 1732 rec/s. Captured in
`docs/BUILD_AGAINST_FLEET_ENGINE.md`, `.claude/faqs/build/engine-build-parity.md`, and `PERFORMANCE.md`
finding #2 (corrected). **Rule: verify the feeder's runtime `apiVersion` (`get_stats`) == the fleet's
before trusting any loader A/B.**

## Fleet deployment (`.142`, operational — NOT part of this git push)

`.142` runs the parallel feeder against the shared live `g2` PG18 DB via the DEV-engine image
`brian/sz_spark:local-b6621d47-dev-pq17` (feeder + redo + drainer all on it; one clean redo loop).
Deploy config lives on the NAS (`/public_data/perfscripts/sz_spark/env.sh`,
`RECORDS_PER_BATCH=1000` / `MAX_UNPROCESSED_BATCHES=CORES_MAX+32` / `SHARD_RECORDS=1000`), not in this
repo. The broader Sayari-load run state is tracked in the **dbperf_test** project's STATUS/NEXT_STEPS,
not here.

## Tests

`sbt test` — 102 unit tests green (was 78 at M16; +dead-letter/StatsPlugin/parallel-feeder specs).
Integration tests (`IntegrationTest`-tagged) require a real engine and are excluded from the unit gate.

## Known gaps (not blocking this push)

1. **Step 2 not built** — Kafka Source (offset watermark) + Delta Source + throttled RabbitMQ→Kafka
   bridge. Same `RecordSource` seam, same `AddCore`; only the source differs.
2. **`.142`-Rust baseline untested** — the parity measurement is `.142`-Spark vs `.141`-Rust, still
   confounded by the ~9% host asymmetry. The clean baseline is Rust on `.142` (same host) — not run.
3. **`SparkRecordOps` 3-jobs/batch** — collapse to one-pass sink (minor; DB-bound arm hides it).
4. Pre-existing: multi-node cluster validation, MSSQL/MySQL DDL, `reinitialize()` concurrency cert.

## Uncommitted state

This session's tree changes, pending review before the push:
- `docs/BUILD_AGAINST_FLEET_ENGINE.md` (new), `.claude/faqs/build/engine-build-parity.md` (new)
- `docs/PERFORMANCE.md` (finding #2 corrected + methodology note), `docs/PARALLEL_BATCH_FEEDER.md`
  (A/B parity caveat), `CHANGELOG.md` (parallel-feeder feature + Docs entries)
- `STATUS.md` + `NEXT_STEPS.md` (this handoff)

Global rule: **ask the user before committing/pushing.** Nothing is lost — git tracks all files and
the full diff is reviewable before approval.
