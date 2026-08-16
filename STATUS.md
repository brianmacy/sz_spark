# Project Status

**Date:** 2026-08-16
**Branch:** `main` — **v0.2.0 released** (tag `v0.2.0` on merge commit `a6b34f1`, GitHub release published).
Working tree clean. Last shipped PR: **#17** (Kafka producer + idempotent DLQ re-drive).

## ★ v0.2.0 — shipped (main @ `a6b34f1`, tag `v0.2.0`)

The Kafka path is now self-contained on-prem and the dead-letter re-drive is idempotent. Landed via
PR #17 (merged 2026-08-16), released as **v0.2.0** (signed annotated tag; `gh release` published from
the CHANGELOG `[0.2.0]` notes). `build.sbt` version is `0.2.0` (was drifted at `0.1.0` behind the
`v0.1.2` tag — now reconciled and ahead of all prior tags).

- **`glue.FileToKafka`** — on-prem JSONL(.bz2/.gz)→Kafka producer (each line = one message value;
  512 MiB caps, `acks=all`). The maintained replacement for the retired bridge: produce with
  `FileToKafka`, consume with `KafkaSource`, no RabbitMQ hop.
- **`glue.KafkaLag`** — plain-JVM processing-progress monitor (topic HWM vs the feeder's checkpoint
  offset; `kafka-consumer-groups.sh` shows nothing since the feeder never commits group offsets).
- **Idempotent DLQ re-drive (`DeadLetterReprocess`)** — snapshots shards, re-emits reprocessable rows,
  then **archives** swept shards out of the dead-letter dir, so a second pass re-emits nothing (prior
  behavior re-emitted every shard forever). DLQ rows are stamped `failedAt` + `source` at both sinks.
- **Retired `glue.MqToKafka`** — the dev-time RabbitMQ→Kafka bridge; `MaxRecordBytes` moved to
  `KafkaSource`. (The kafka-clients / spark-sql-kafka deps stay — used by `KafkaSource`/`FileToKafka`/
  `KafkaLag`.)

### Validation at release
- **CI `build-and-test` PASS** on the merge commit (runs `sbt test` — the unit suite is green).
- **`EngineIT` 5/5** verified locally this session against the licensed engine (`/opt/senzing`, throwaway
  SQLite, `SZ_IT=1`) via `scripts/it-local.sh` — the real-engine gate PR #17's surface had never run.
- Tree clean; the profiling-only `NativeStaging` `SZ_NO_STRIP=1` change was **discarded** (not shipped).

### Known CHANGELOG editorial follow-ups (non-blocking, content is accurate)
The `[0.2.0]` section is dev-PR-ordered: duplicate subsection headers (Added×2, Changed×3) and
`glue.MqToKafka` appears in both Added (its dev-line introduction) and Removed (its retirement) of the
same release. Content is complete and accurate; a future patch can consolidate into single
Added/Changed/Removed/Docs subsections and collapse the intra-release MqToKafka churn.

## Prior work on `main` (context)

- **Entity-mart replication Phase 1 + 1.1 (PR #10) + Phase 2 (PR #11) MERGED** — `EntityMartSchema`,
  `GetCore`, `EntityMartRows`, `EntityMartSink`/`LocalDeltaSink`, `EntityMartSync`, `DatabricksUcSink`
  (Unity-Catalog target), change-gate + orphan reconcile, tutorial Guide 07, Databricks deploy docs.
  `EntityMartSinkIT` 5/5. Remaining = **Phase 3 live-infra validation** (needs a real Databricks cluster /
  the fleet, not code) — see NEXT_STEPS.
- **Feeder auto-recovery `FeederSupervisor` (PR #13) MERGED** — unit-validated; **NOT yet exercised
  against the live engine on `.142`** (deploy/IT still pending) — see NEXT_STEPS.
- **Source-agnostic overlapping-batch feeder + Kafka/Delta sources** — `ParquetParallelFeeder`,
  `KafkaSource`, `DeltaSource`, `OverlappingBatchEngine`, dead-letter capture (all in v0.2.0's CHANGELOG).

## Background / operational state (fleet — NOT session tasks, verify don't rebuild)

The `.142` Kafka cutover and the Sayari dual-host load are operational on the NAS
(`/public_data/perfscripts/sz_spark/`), tracked in dbperf_test `.claude/SAYARI_LOAD_STATUS.md`.
⛔ Do NOT run `run-drainer.sh` (retired parquet path). Do NOT stop `sz-kafka` / the publisher on `.100`.
Do NOT touch the spark_er ODO run.
