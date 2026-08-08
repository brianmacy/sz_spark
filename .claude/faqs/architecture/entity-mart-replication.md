# Entity-mart replication (real-time Databricks export)

**What it is.** The `mart/` package — a sink that projects the resolved entity graph into
read-optimized Delta tables so other Databricks jobs/users can query the latest entities and
relationships in near-real-time. It is NOT a second engine pass: it rides the affected-entity change
feed the ingest feeders already emit (`WITH_INFO` → `AFFECTED_ENTITIES`), so every `add_record` that
moves an entity triggers exactly the re-projection of the entities it touched.

**Design of record:** `~/.claude/plans/sz_spark_entity_map_delta_replication.md` (GetCore → EntityMartRows
→ EntityMartSink seam, LocalDeltaSink/DatabricksUcSink, EntityMartSync/Snapshot/Reconcile, flag set §7.4,
open questions O1–O6).

## The flow

1. **Ingest feeders** (`ParquetParallelFeeder`, `source=kafka|parquet|delta`) already write the affected
   entity-ids of every batch to `output=$AFFECTED` (parquet). That is the change feed — no new engine
   pass on the load path.
2. **`EntityMartSync`** reads that feed, dedups ids per micro-batch, and hands them to `GetCore`.
3. **`GetCore`** is the engine bracket (one `SzEnvironment` per executor JVM, exactly like
   `SparkRecordOps`): `getEntity(id, REPLICATION_FLAGS)` per affected id → tagged row
   `(entity_id, kind, json)`, `kind ∈ {ENTITY, GONE, ERROR}`. `GONE` (`SzNotFoundException`) = the entity
   was absorbed/deleted ⇒ **tombstone**; `ERROR` ⇒ `_quarantine`.
4. **`EntityMartRows`** (pure, golden-file-tested) Jackson-parses each ENTITY json → the four frames +
   a canonical `entity_hash` change-gate; GONE ids → the tombstone frame.
5. **`EntityMartSink`** (`LocalDeltaSink` now, `DatabricksUcSink` later) — four idempotent
   `MERGE INTO ... WHEN MATCHED AND source.refresh_seq >= target.refresh_seq` upserts + tombstone deletes.

## The schema (`EntityMartSchema`)

Four projections + two bookkeeping tables, one `CREATE TABLE` DDL shape for both a **path-based
OSS-Delta** table (`` delta.`/path/entity` `` — the local proxy) and a **Unity Catalog 3-part name**
(Databricks); the only difference is the locator the sink passes.

| Table          | Grain                          | Cluster key                        |
| -------------- | ------------------------------ | ---------------------------------- |
| `entity`       | one row / resolved entity      | `entity_id`                        |
| `entity_record`| the ENTITY MAP: one / record   | `data_source, record_id, entity_id`|
| `relationship` | entity↔entity, `lo<hi`         | `entity_id_lo, entity_id_hi`       |
| `entity_doc`   | `entity_id → full entity JSON` | `entity_id`                        |
| `_sync_state`  | watermarks                     | —                                  |
| `_quarantine`  | GetCore failures (dead-letter) | —                                  |

**Why these table properties:** all mutation-heavy tables enable **deletion vectors** (MERGE/DELETE
without rewriting whole files), **Change Data Feed** (the downstream serving hook), and **liquid
clustering** on the MERGE key (`entity_id` is high-cardinality; classic partitioning would fragment MERGE
writes). Every mart table carries `refresh_seq BIGINT` + `updated_at TIMESTAMP` so a delayed/replayed
batch is a no-op (`source.refresh_seq >= target.refresh_seq`).

⚠ **Phase-1 assumption to verify on the local rig** (design §10): OSS delta-spark 4.0 must support
`CLUSTER BY`, deletion vectors, and CDF. If any is unsupported locally, the smoke test drops the
corresponding clause (`EntityMartSchema.clusterBy` / `.tableProperties` isolate them).

## Status (2026-08-08)

Phase-1 **built + tested** on branch `bem_entity_mart`: `EntityMartSchema`, `GetCore`, `EntityMartRows`
(+ 7-case `EntityMartRowsSpec`), `EntityMartSink`/`LocalDeltaSink`, `EntityMartSync`. Default suite
129/0; `EntityMartSinkIT` (local Spark+Delta, no engine) 3/3.

**Two OSS delta-spark gotchas the sink IT surfaced (both fixed):**
- **DELETE does not support `IN (subquery)`** (`DELTA_UNSUPPORTED_SUBQUERY`). Express a delete-by-join
  as `MERGE INTO t USING ids ON <join> WHEN MATCHED THEN DELETE`. The tombstone cascade does this.
- **`relationship` MERGE must be column-wise `coalesce(source, target)`, NOT `UPDATE SET *`.** Each
  endpoint's refresh carries only ITS direction (`match_key` when `lo`, `rev_match_key` when `hi`); a
  blind `SET *` nulls the other, so the two directions ping-pong and never coexist. `coalesce` keeps
  both (and for always-present columns reduces to latest-wins under the `refresh_seq` guard).
- **Confirmed:** the `CLUSTER BY` (liquid clustering) + deletion-vectors + CDF `CREATE TABLE` DDL runs
  on OSS delta-spark 4.0.0 locally (design §10 assumption — no fallback needed).

**Run the sink IT** (tag-excluded from `sbt test`, and the build's global `-l IntegrationTest` also
blocks a `-n` include): `sbt 'set Test/testOptions := Seq()' "testOnly *EntityMartSinkIT"`.

Assumed answers pending user confirm: O1 → §7.4 flags (no raw `JSON_DATA`/candidate features), O2 →
pure-sz_spark Databricks target (feed complete by construction), O4 → configurable sync cadence.

## Alignment with the Senzing data-mart-replicator (authority: Senzing-MCP `reporting_guide data_mart`)

This design follows the documented **Entity Refresh Pattern**: SZ_WITH_INFO → AFFECTED_ENTITIES →
dedup → `getEntity` current state → apply idempotently. **We deliberately re-fetch rather than apply the
WITH_INFO delta** — the docs are explicit that under parallel processing you CANNOT know the order of
operations, so ordered delta application would desync; re-fetching current state is order-independent
(our `refresh_seq` guard adds protection against an out-of-order batch clobbering a newer write).

MATCHES the reference: explicit replication flags (never `*_DEFAULT_FLAGS`); tombstone on
entity-not-found; **`relationship` normalized `lo<hi` storing BOTH `match_key` + `rev_match_key`** (the
doc mandates both since relationships are asymmetric — our coalesce MERGE is precisely this); dedup ids
per batch; denormalized tables (NOT the "raw JSON blob as schema" anti-pattern); canonical sorted-key
change hash.

Reference gaps status (tracked in NEXT_STEPS): (1) **✅ hash change-gate wired** —
`EntityMartSink.selectChanged` drops entities whose stored `entity_hash` is unchanged before frames are
built ("skip if unchanged"); the canonical hash uses US/RS separators (collision-safety). (2) **✅
orphan-record reconcile** — `reconcileDepartedRecords` removes an `entity_record` row when its record
leaves a surviving entity (delete); a MOVE is re-keyed by the gaining entity's refresh. ⚠ Phase-2
hardening: if the gaining entity of a move is not in the same batch the row is briefly deleted +
re-inserted (eventual consistency) — the reference's `getRecord`-verify sweep avoids the transient. (3)
the aggregate report tables (`sz_dm_report` DSS/CSS/ESB/ERB + deferred `sz_dm_pending_report`) are
**deliberately not built — a different mart archetype, not a gap.** The reference's `sz_dm_report` is the
*analytics/reporting* mart style (pre-aggregated ER-quality stats). Ours is the *entity/relationship
serving-map* style (the live denormalized entity + record + relationship projections a Databricks job or
user queries directly). `sz_dm_report` is one pattern for one mart style; a lakehouse consumer aggregates
over our map with the doc's own SQL patterns (which run against exactly our `entity`/`entity_record`/
`relationship` shape). Confirmed acceptable 2026-08-08. Both (1) and (2) are proven by `EntityMartSinkIT`
(5 cases).

Our column naming differs from the reference (`entity`/`entity_record`/`relationship` +
`entity_id_lo`/`entity_id_hi` vs `sz_dm_entity`/`sz_dm_record`/`sz_dm_relation` + `related_id`); the
reference's single `match_type` (AM/PM/DR/PR) we carry as `match_level_code` + `is_ambiguous`/`is_disclosed`.
