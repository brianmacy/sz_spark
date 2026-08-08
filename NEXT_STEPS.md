# Next Steps

**Branch:** bem_mart_phase2 — entity-mart replication **Phase 2**. Design of record:
`~/.claude/plans/sz_spark_entity_map_delta_replication.md`. Phase 1 + 1.1 are **MERGED** (PR #10).

## NOW: Phase 2 — DatabricksUcSink + tutorial (this branch) → PR → merge

DONE on this branch: `DatabricksUcSink` (Unity-Catalog target — same `AbstractDeltaSink` MERGE/DELETE
logic, UC `catalog.schema.\`table\`` naming), `EntityMartSync sink=local|uc` wiring, `DatabricksUcSinkSpec`
(suite **133/0**), tutorial **Guide 07** (`docs/tutorial/07-entity-mart-replication.md`) +
`docs/DATABRICKS.md` §Entity-mart. Next: PR this branch → monitor CI → merge. Remaining Phase-2/3 items
(need live infra / new design) are below.

## Entity-mart replication — Phase 1 DONE (all in `mart/`)

`EntityMartSchema` + `GetCore` + `EntityMartRows` (+ 7-case `EntityMartRowsSpec`) + `EntityMartSink`/
`LocalDeltaSink` + `EntityMartSync` are built, compiling, and tested (default suite 129/0; the
`EntityMartSinkIT` Delta IT 3/3). Two fixes the IT surfaced and proved: the `relationship` MERGE is
column-wise `coalesce` (a single-endpoint refresh no longer nulls the opposite direction), and the
tombstone cascade uses `MERGE ... WHEN MATCHED THEN DELETE` (OSS delta-spark rejects `IN (subquery)`
in `DELETE`). The IT also confirmed the `CLUSTER BY`+DV+CDF DDL runs on OSS delta-spark 4.0.0 (design
§10 assumption — CONFIRMED).

### Best-practice alignment vs the Senzing data-mart-replicator (from the Senzing-MCP)

`reporting_guide topic=data_mart` + the "Advanced Real-time Replication" tutorial are the authority.
We MATCH the pattern on: SZ_WITH_INFO→AFFECTED_ENTITIES feed; the **Entity Refresh Pattern**
(re-fetch current state, apply idempotently — NOT ordered delta application, which the docs warn is
impossible under parallel processing); explicit replication flags (not `*_DEFAULT_FLAGS`); tombstone on
entity-not-found; **relationship normalization `lo<hi` storing BOTH `match_key` and `rev_match_key`**
(the doc mandates both — our coalesce MERGE is exactly this); dedup affected ids per batch; denormalized
tables (not JSON-blob-as-schema); canonical sorted-key hash for change detection.

1. **✅ DONE (Phase 1.1) — hash change-gate wired.** `EntityMartSink.selectChanged` drops entities whose
   stored `entity_hash` equals the fresh one before frames are built, so an unchanged re-resolution
   writes nothing across all four tables (the doc's "skip if unchanged"). The canonical hash now uses
   US/RS separators (collision-safety). Proven by `EntityMartSinkIT` (change-gate case).
2. **✅ DONE (Phase 1.1) — orphan-record reconcile.** `reconcileDepartedRecords` deletes an
   `entity_record` row when its record leaves a surviving entity (delete); a MOVE is re-keyed by the
   gaining entity's refresh. Proven by `EntityMartSinkIT` (orphan case). ⚠ Phase-2 hardening: if the
   gaining entity of a move is not in the SAME batch, the row is briefly deleted then re-inserted on that
   entity's refresh (eventual consistency) — the reference avoids the transient via a `getRecord`-verify
   sweep; add that if a consumer can't tolerate the brief gap. Also perf: the reconcile reads
   `entity_record WHERE entity_id IN (batch ids)` — not cluster-pruned (clustering is on
   `data_source, record_id, entity_id`); a secondary index or delta-based departed detection is a Phase-2
   optimization.
3. **Aggregate report tables — a different mart archetype, not a gap (RESOLVED 2026-08-08).** The
   reference's `sz_dm_report` (DSS/CSS/ESB/ERB via +1/-1 deltas + a `sz_dm_pending_report` queue) is the
   *analytics/reporting* mart style; ours is the *entity/relationship serving-map* style. `sz_dm_report`
   is one pattern for one mart style — confirmed acceptable to serve the denormalized map and let
   Databricks aggregate over it (the doc's SQL patterns run against our exact shape). No action.

### Remaining (Phase 2 / 3) — DONE this branch: change-gate, orphan reconcile, DatabricksUcSink
1. **Runtime smoke on the fleet** — launch `EntityMartSync` against a live affected feed
   (`--packages io.delta:delta-spark_2.13:4.0.0`; `feed=` a feeders' `output=$AFFECTED` dir; `mart=` a
   Delta base, or `sink=uc mart=catalog.schema staging=<Volume>`); verify rows land. Write a
   `run-entity-mart.sh` on the NAS — **not run yet** (fleet busy draining the ~234M Kafka backlog).
2. **Live-UC smoke for `DatabricksUcSink`** — validate the `catalog.schema` path on a real Databricks
   cluster (locally unvalidatable; the shared `AbstractDeltaSink` logic is IT-proven and
   `DatabricksUcSinkSpec` covers the naming, so this is the only unverified surface).
3. **Orphan reconcile hardening** — `getRecord`-verify before an orphan delete (avoids a brief
   cross-batch move transient); reconcile-read pruning (the `entity_id IN (...)` read isn't
   cluster-pruned — clustering is on `data_source, record_id, entity_id`).
4. **Run `EntityMartSinkIT` in CI** — tag-excluded from `sbt test`; the build's global `-l IntegrationTest`
   also blocks a `-n` include, so run it with `sbt 'set Test/testOptions := Seq()' "testOnly *EntityMartSinkIT"`.
   Consider a dedicated `delta-it` tag so CI runs it without a live engine.

**Confirm with user** (plan open questions, assumed): O1 → §7.4 flags; O2 → pure-sz_spark Databricks
target; O4 → configurable cadence.

## Operational (fleet — verify, don't rebuild)

- **Verify the ~234M Kafka backlog drains** on `.142` (`sz-spark-feeder`, ~65 h, DB-bound ~1k/s). When
  the feeder's committed offset closes within `MAX_LAG=5M` of the topic head, the **bridge resumes**
  (`get/s` goes non-zero) — that's the steady-state Kafka path. Watch RabbitMQ (`.100:15672`).
- **`huge/` residual** — confirm `sz-spark-feeder-huge` (local one-shot) processed the ~1-2 giant
  records and exited on `availableNow`. If it starved, it needs the cluster idle or a local rerun.
- ⛔ Do NOT run `run-drainer.sh` (retired parquet path). Do NOT stop `sz-kafka` on `.100` or the publisher.
