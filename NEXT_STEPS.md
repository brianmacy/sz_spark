# Next Steps

## ★ NEXT (2026-08-07) — PARALLEL-BATCH FEEDER (fix the Spark straggler tail)

The micro-batch **straggler tail** idles the cluster (measured `.142` 76% idle — a few huge-entity records stall the
whole batch). Replace `ParquetStreamFeeder` with a source-agnostic **overlapping-batch** feeder: custom FAIR-scheduler
driver, single reader, unbounded read-ahead — **NOT Structured Streaming** (SS is strictly sequential). **Full plan +
locked decisions:** `~/.claude/plans/sz_spark_parallel_batch_feeder.md`.
- **STEP 1 — RabbitMQ path:** new glue `ParquetParallelFeeder` over the parquet inbox (atomic-rename claim → `AddCore`
  → dispose-on-completion → reclaim-orphans-on-restart); reuse the `MqToParquet` drainer (ack-on-persist) unchanged;
  deploy on `.142` replacing `ParquetStreamFeeder`; measure the idle drop + tail-gone.
- **STEP 2 — Kafka + bridge:** Kafka Source (offset cursor, `minPartitions` on ONE unpartitioned topic, monotonic
  watermark) + a RabbitMQ→Kafka adapter throttled so the Spark consumer stays ≤ ~5M records behind.
Locked (don't re-litigate): overlapping-batches (NOT over-decomposition / NOT worker-pull); source seam
`{nextChunk, read→DataFrame, commit}` with per-unit-dispose vs monotonic-watermark flavors; `engine.ConfigDrift` is
the sole reinit (keep it).

---

## Immediate (pre-merge) — ✅ DONE (PR #4 merged to `main` 2026-08-07); kept for history

1. **User reviews and approves the current changelist** (branch `bem_rabbitmq_ingest`): dead-letter
   capture + `DeadLetterReprocess`, the `diag.StatsPlugin` `getStats` sampler, the per-batch
   repartition removal, and the accompanying docs.
   - Run `git diff --stat` / `git status` for a full tree view.
   - Nothing is staged; no destructive operations will run without explicit approval.
   - ⚠ Before committing, decide on the customer-codename `DATA_SOURCE` literals in
     `ParquetStreamFeederSpec.scala` — a codename in a committed test fixture. Replace with a neutral
     value (e.g. `TEST_DS`) if it should not ship.

## Streaming ingest (this branch)

1a. **Build Stage 1 `RabbitMqSource`** — the plain-JVM MQ→parquet drainer (persist-then-ack) is
    designed in `docs/RABBITMQ_INGEST.md` but not yet implemented; only the Stage 2 feeder exists.

1b. **Exercise the feeder end-to-end** on a cluster: dead-letter capture on real failures, then
    `DeadLetterReprocess` replay; confirm the `SZ_STATS` sampler lands one central stream in the
    driver log with `--conf spark.plugins=com.senzing.spark.diag.StatsPlugin`.

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
