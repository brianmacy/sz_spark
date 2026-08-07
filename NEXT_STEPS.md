# Next Steps

## ★ STEP 1 — PARALLEL-BATCH FEEDER — ✅ DONE (branch `bem_parallel_batch_feeder`, `87f699d`)

The micro-batch **straggler tail** idled the cluster (measured `.142` 76% idle). Replaced
`ParquetStreamFeeder` with a source-agnostic **overlapping-batch** feeder (custom FAIR-scheduler driver,
K worker threads, 1 partition/batch) — implemented, 102/102 unit tests, and deployed on `.142` against
the live `g2` DB. The apparent ~20% deficit vs the Rust fleet was a **stale engine baked into the jar**,
NOT the feeder — rebuilding against the fleet's `4.4.0.DEVELOPMENT` engine restored parity (CPU
44%→55.5% ≈ Rust 57%). See `docs/PARALLEL_BATCH_FEEDER.md`, `docs/BUILD_AGAINST_FLEET_ENGINE.md`, and
`STATUS.md`. Plan of record: `~/.claude/plans/sz_spark_parallel_batch_feeder.md`.

## ★ NEXT — push this branch, then Step 2 (Kafka)

1. **Push `bem_parallel_batch_feeder`** after user review of the changelist (feeder + docs/FAQ/CHANGELOG
   updates this session). No PR exists yet; open one on push.
2. **`.142`-Rust baseline** — run the Rust consumer on `.142` (same host) to remove the ~9% host-asymmetry
   confound from the parity number (currently `.142`-Spark vs `.141`-Rust).
3. **STEP 2 — Kafka + bridge:** `KafkaSource` (offset cursor, `minPartitions` fanning ONE unpartitioned
   topic into N tasks, monotonic-watermark commit) + `DeltaSource` (version/CDF watermark) implementing
   the same `RecordSource` seam, plus a RabbitMQ→Kafka adapter throttled so the Spark consumer stays
   ≤ ~5M records behind. Same engine, same `AddCore` — only the source differs.
4. **Collapse `SparkRecordOps` 3-jobs/batch** to a one-pass sink (minor; the DB-bound arm hides it today).

Locked (don't re-litigate): overlapping-batches (NOT over-decomposition / NOT worker-pull); source seam
`{nextChunk, read→DataFrame, commit}` with per-unit-dispose vs monotonic-watermark flavors; `engine.ConfigDrift`
is the sole reinit (keep it); 1 partition/batch + `maxUnprocessedBatches` ≥ `spark.cores.max`.

## Streaming ingest — ✅ landed (PR #4 on `main`; kept for history)

1a. **Stage 1 MQ→parquet drainer** (`glue.MqToParquet`, persist-then-ack) — DONE; running on `.142`.
    `docs/RABBITMQ_INGEST.md` describes the two-stage path.

1b. **Exercise the feeder end-to-end** on a cluster: dead-letter capture on real failures, then
    `DeadLetterReprocess` replay; confirm the `SZ_STATS` sampler lands one central stream in the
    driver log with `--conf spark.plugins=com.senzing.spark.diag.StatsPlugin`. (Streaming feeder done;
    the parallel feeder now supersedes it — see the top block.)

2. **Hash-pin CI actions** — ✅ DONE. `.github/workflows/ci.yml` SHA-pins the latest majors
   (`actions/checkout@…9c091bb # v7.0.0`, `actions/setup-java@…1bcf9fb # v5.4.0`,
   `actions/cache@…55cc834 # v6.1.0`). `.github/dependabot.yml` covers `github-actions` only —
   Dependabot has no sbt support, so Scala/sbt deps are bumped manually (all at latest stable as of
   2026-06-30; sbt kept on the 1.x line since 2.0 is a breaking major).

3. **Wire a licensed Senzing dist into CI** — ✅ DONE. `ci.yml` now installs `senzingsdk-runtime`
   at CI time from Senzing's public apt repo (`senzing-production-apt.s3.amazonaws.com`, mirroring
   the official `senzing/senzingsdk-runtime` Dockerfile) into `/opt/senzing` on the hosted
   `ubuntu-latest` runner. No self-hosted runner, no committed SDK, no redistribution. The
   `Verify Senzing SDK` step still fails fast if the jar is absent after install.

## Short-term (next session)

4. **Validate DRAFT deployment tutorials on real clusters** — `docs/tutorials/spark-onprem.md`, `aws-emr.md`, and `databricks.md` are marked DRAFT and have not been tested on live infrastructure. Walk through each tutorial on its target platform; fix any discrepancies; remove the DRAFT warning header when validated. Update the `tutorials.md` FAQ entry to reflect status.

5. **Multi-node cluster validation** — run `AddUpdateJob` against a real Spark standalone cluster (or Databricks) with ≥2 executors on a shared PostgreSQL instance. Confirm per-JVM singleton behavior, no cross-task env conflicts.

6. **MSSQL / MySQL DDL** — add `SchemaApplier` DDL files for both dialects and exercise via `InitSpec` or a dedicated IT.

7. **`reinitialize()` concurrency certification** — confirm with Senzing support or the MCP that `reinitialize(id)` is safe when other threads hold the engine read-lock and are mid-verb. Document the answer in the threading FAQ.

8. **Dependabot review cadence** — after the first commit, dependabot will open its first PRs within a week. Establish a review/merge cadence; the 21-day cooldown gives breathing room.

## Medium-term

9. **Databricks cluster policy docs** — `docs/DATABRICKS.md` covers init scripts and cluster config. Validate against a real DBR 14+ cluster and update as needed.

10. **Redo dedicated worker** — the current `RedoJob` drains redo as a standalone Spark job. Evaluate whether a single-threaded dedicated redo worker (non-Spark) is preferable for high-throughput scenarios where per-task drain causes contention. See FAQ `redo/redo-strategy`.

11. **Performance benchmarks** — run `AddUpdateJob` on a representative dataset (≥1M records, 4+ executors) and capture records/sec, redo queue depth, and GC pressure. Add to `docs/DESIGN.md`.
