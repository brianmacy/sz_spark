# Next Steps

**Branch:** `main` — **v0.2.0 released** (tag `v0.2.0` @ merge commit `a6b34f1`). No open PR.
End-user surface is shipped and consumable: `main` at a tagged v0.2.0 with CI-green unit tests,
`EngineIT` 5/5, and current user-facing docs (README, CHANGELOG, tutorial Guides, kafka/DLQ FAQ).

## NOW: validate + deploy feeder auto-recovery on `.142` (PR #13, merged, unit-only)

1. **Rebuild the `.142` feeder jar** to carry `FeederSupervisor` (the deployed jar predates it; `.142`
   currently relies only on Spark-native `--conf` recovery). Use the arena-engine `IMAGE=` override — a
   run-script without it reverts `.142` off the arena engine.
2. **Live-engine IT** — the shipped unit suite is mocked (Mockito, no engine); exercise the supervisor
   against the real engine on `.142` (kill an executor mid-batch, confirm the run restarts from the
   committed offset with no lost/duplicated records beyond at-least-once).

## NEXT: entity-mart replication Phase 3 — live-infra validation (code + docs already merged)

All Phase 1/2 code + docs are on `main` (suite green, `EntityMartSinkIT` 5/5). Remaining needs LIVE
infrastructure, not code:
1. **Live-UC smoke** of `EntityMartSync sink=uc mart=<catalog.schema> staging=<Volume>` on a real
   Databricks cluster (locally unvalidatable; `DatabricksUcSinkSpec` covers naming, the shared
   `AbstractDeltaSink` logic is IT-proven).
2. **Fleet runtime smoke** of `EntityMartSync` against a live affected feed (write `run-entity-mart.sh`
   on the NAS; `--packages io.delta:delta-spark_2.13:4.0.0`; `feed=` a feeders' `output=$AFFECTED` dir) —
   deferred while the fleet drains the Kafka backlog.
3. **Orphan reconcile hardening** — `getRecord`-verify before an orphan delete (avoids a brief
   cross-batch move transient); reconcile-read pruning (the `entity_id IN (...)` read isn't
   cluster-pruned — clustering is on `data_source, record_id, entity_id`).
4. **Run `EntityMartSinkIT` in CI** — tag-excluded from `sbt test`; run with
   `sbt 'set Test/testOptions := Seq()' "testOnly *EntityMartSinkIT"`. Consider a dedicated `delta-it`
   tag so CI runs it without a live engine.

## Optional polish (non-blocking)

- **Consolidate the `[0.2.0]` CHANGELOG section** into single Added/Changed/Removed/Docs subsections and
  collapse the intra-release `MqToKafka` add+remove churn (accurate now, just dev-PR-ordered). Do it in a
  patch release, not by re-cutting the v0.2.0 tag.

## Operational (fleet — verify, don't rebuild)

- **Sayari dual-host load** + the `.142` Kafka backlog drain are tracked in dbperf_test
  `.claude/SAYARI_LOAD_STATUS.md` — verify progress there; don't rebuild.
- ⛔ Do NOT run `run-drainer.sh` (retired parquet path). Do NOT stop `sz-kafka` / the publisher on `.100`.
  Do NOT touch the spark_er ODO run.

**Confirm with user** (entity-mart plan open questions, assumed): O1 → §7.4 flags; O2 → pure-sz_spark
Databricks target; O4 → configurable cadence.
