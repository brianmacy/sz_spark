# Project Status

**Date:** 2026-08-09
**Branch:** main — entity-mart replication **Phase 1 + 1.1 (PR #10) and Phase 2 (PR #11) both MERGED**.
**State:** the entity-mart is complete through Phase 2 on `main`: `EntityMartSchema`, `GetCore`,
`EntityMartRows`, `EntityMartSink`/`LocalDeltaSink`, `EntityMartSync`, `DatabricksUcSink` (Unity-Catalog
target), + the change-gate/orphan-reconcile, tutorial **Guide 07**, and Databricks deploy docs. Default
suite **133/0**, `EntityMartSinkIT` 5/5. Remaining work is **Phase 3 — live-infra validation** (needs a
real Databricks cluster / the fleet, not code); see NEXT_STEPS. Separately landed this session: the `.142`
Kafka cutover (operational, NAS) and the dbperf PG18 work (dbperf PR #15, merged).

## Entity-mart replication — Phase 1 (PR #10) + Phase 2 (PR #11) MERGED

Design of record: `~/.claude/plans/sz_spark_entity_map_delta_replication.md`. Driven by the
affected-entity feed (`WITH_INFO` → `AFFECTED_ENTITIES`) the feeders already emit → per affected id
`getEntity` → MERGE into Delta tables.

- **Phase 1 DONE (all `mart/`):** `EntityMartSchema` (Delta DDL), `GetCore` (engine bracket → explicit
  §7.4-flag `getEntity` → tagged ENTITY/GONE/ERROR, mirrors `SparkRecordOps`), `EntityMartRows`
  (Jackson transform → four frames + tombstones + canonical hash; 7-case `EntityMartRowsSpec`),
  `EntityMartSink`/`LocalDeltaSink` (monotonic MERGEs + tombstone cascade), `EntityMartSync` (glue
  driver). Default suite **129/0**; the `EntityMartSinkIT` local Spark+Delta IT **3/3**.
- **Sink IT (5/5) caught + proved four things:** `relationship` MERGE is column-wise `coalesce` (a
  single endpoint's refresh no longer nulls the opposite direction); tombstone cascade uses
  `MERGE ... WHEN MATCHED THEN DELETE` (OSS delta-spark rejects `IN (subquery)` DELETE); the **hash
  change-gate** skips unchanged entities; the **orphan-record reconcile** removes a record deleted from a
  surviving entity. It also **confirmed `CLUSTER BY`+DV+CDF DDL runs on OSS delta-spark 4.0.0** (§10).
- **Best-practice alignment (Senzing-MCP data-mart-replicator):** matches the Entity Refresh Pattern,
  explicit flags, tombstoning, relationship-both-directions, dedup, denormalized tables, canonical hash +
  change-gate + orphan handling. The `sz_dm_report` aggregate tables are a different mart archetype
  (analytics-reporting), not a gap — ours is the entity/relationship serving-map style; confirmed
  acceptable 2026-08-08. See the entity-mart FAQ.
- **Next (Phase 1.1 / 2, see NEXT_STEPS):** runtime smoke on the fleet (`run-entity-mart.sh` +
  `--packages delta-spark`); `DatabricksUcSink`; Phase-2 orphan `getRecord`-verify + reconcile perf.
- **Assumed answers to the plan's open questions** (confirm with user): O1 → the §7.4 flag set
  (no raw `JSON_DATA`/candidate features); O2 → build for the **pure-sz_spark Databricks** target
  (feed complete by construction); O4 → configurable sync cadence knob.

## ★ `.142` cut over to Kafka (operational — NAS + merged code, NOT git)

The `.142` drainer had run **ungated ~14h** (backpressure supervisor wasn't running), acking ~6k/s
into parquet while the engine resolved ~1k/s — a **~235M-record parquet backlog** built up (hidden by
an `ls part-*.parquet` ARG_MAX glob bug that read `inbox=0`). Resolution (all done this session):

- **Migrated ~233.9M records parquet→Kafka** (topic `senzing-records` on `.100`, size-split: ≤15 MB →
  Kafka, the 1-2 giant >15 MB records set aside to `/data/tmp/sz_spark_io/huge/`). Deleted the migrated
  parquet → **freed 89 G on `.142`**.
- **`.142` now runs the Kafka path:** `sz-spark-feeder` (`source=kafka`, draining the ~234M backlog at
  ~1k/s DB-bound → **~65 h**, 0 failed), `sz-spark-bridge` (`MqToKafka`, RabbitMQ→Kafka for new records,
  **correctly throttled** at `get/s=0` while the feeder is >5M behind), `sz-spark-redo`.
- **`.141`** unaffected (~960/s). The affected-entity feed the kafka feeder emits carries every resolved
  entity, so the entity-mart replication sees the backlog + new records regardless of source.

Fixes made (all on the NAS `/public_data/perfscripts/sz_spark/`): `run-drainer.sh` gated-by-default;
`SHARD_RECORDS`=1000 (was 5000 → 5× oversized batches); `run-feeder-kafka.sh`/`run-bridge.sh` got
`--packages spark-sql-kafka-0-10` + ivy(`/tmp/ivy`) + `HOME=/tmp`; topic `max.message.bytes`=16 MB +
14 d retention. Parity image on `.142`: `brian/sz_spark:local-618b47c2-dev-pq17` (bundled DEV engine).

## Background / operational state at handoff (fleet, NOT session tasks)

- `.142` `sz-spark-feeder` — Kafka feeder draining ~234M (~65 h). Log `feeder-kafka.out`.
- `.142` `sz-spark-bridge` — throttled bridge. Log `bridge.out`. **Do NOT** run `run-drainer.sh` (parquet
  path retired; would re-ack RabbitMQ into parquet).
- `.142` `sz-spark-feeder-huge` — one-shot local drain of the 25 M huge residual; exits on `availableNow`.
- `.142` `sz-spark-redo` — redo loop (`run-redo.sh`). `.100` `sz-kafka` — the Kafka broker (do NOT stop).
- `.100` RabbitMQ publisher — do NOT kill. `.141` Rust fleet — untouched.

## Uncommitted / next-session

Working tree: `mart/EntityMartSchema.scala` (committed on this branch as the Phase-1 start). The Kafka
cutover run scripts live on `/public_data` (not git). Next: continue the entity-mart Phase-1 build
(order above); verify the ~234M backlog drains; the `huge/` residual exits cleanly.
