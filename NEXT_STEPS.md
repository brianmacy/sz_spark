# Next Steps

**Branch:** bem_entity_mart — entity-mart replication (Phase 1). Design of record:
`~/.claude/plans/sz_spark_entity_map_delta_replication.md`.

## Entity-mart replication — Phase 1 DONE (all in `mart/`)

`EntityMartSchema` + `GetCore` + `EntityMartRows` (+ 7-case `EntityMartRowsSpec`) + `EntityMartSink`/
`LocalDeltaSink` + `EntityMartSync` are built, compiling, and tested (default suite 129/0; the
`EntityMartSinkIT` Delta IT 3/3). Two fixes the IT surfaced and proved: the `relationship` MERGE is
column-wise `coalesce` (a single-endpoint refresh no longer nulls the opposite direction), and the
tombstone cascade uses `MERGE ... WHEN MATCHED THEN DELETE` (OSS delta-spark rejects `IN (subquery)`
in `DELETE`). The IT also confirmed the `CLUSTER BY`+DV+CDF DDL runs on OSS delta-spark 4.0.0 (design
§10 assumption — CONFIRMED).

### Remaining (Phase 1.1 / Phase 2)
1. **Runtime smoke on the fleet** — launch `EntityMartSync` against a live affected feed with
   `--packages io.delta:delta-spark_2.13:4.0.0` (write a `run-entity-mart.sh` on the NAS), point
   `feed=` at a feeders' `output=$AFFECTED` dir, `mart=` at a local Delta base; verify rows land.
2. **Wire the change-gate** — the `entity_hash` is computed + stored but the sink still writes every
   affected entity each batch. Add `AND s.entity_hash <> t.entity_hash` to the `entity`/`entity_doc`
   MERGEs (and give the concat hash a field delimiter first — current `mkString("")` has a
   low-probability boundary-collision risk that only matters once the gate gates on it).
3. **`DatabricksUcSink`** — subclass `AbstractDeltaSink`, override `locator` with UC 3-part names (O2's
   Databricks-native path); the MERGE/DELETE SQL is unchanged.
4. **Run `EntityMartSinkIT` in CI** — it's tag-excluded from `sbt test`; the build's global
   `-l IntegrationTest` in `testOptions` also blocks a `-n` include, so run it with
   `sbt 'set Test/testOptions := Seq()' "testOnly *EntityMartSinkIT"`. Consider a dedicated
   `delta-it` tag so it can run in CI without a live engine (unlike the SZ_IT engine ITs).

**Confirm with user** (plan open questions, assumed): O1 → §7.4 flags; O2 → pure-sz_spark Databricks
target; O4 → configurable cadence.

## Operational (fleet — verify, don't rebuild)

- **Verify the ~234M Kafka backlog drains** on `.142` (`sz-spark-feeder`, ~65 h, DB-bound ~1k/s). When
  the feeder's committed offset closes within `MAX_LAG=5M` of the topic head, the **bridge resumes**
  (`get/s` goes non-zero) — that's the steady-state Kafka path. Watch RabbitMQ (`.100:15672`).
- **`huge/` residual** — confirm `sz-spark-feeder-huge` (local one-shot) processed the ~1-2 giant
  records and exited on `availableNow`. If it starved, it needs the cluster idle or a local rerun.
- ⛔ Do NOT run `run-drainer.sh` (retired parquet path). Do NOT stop `sz-kafka` on `.100` or the publisher.
