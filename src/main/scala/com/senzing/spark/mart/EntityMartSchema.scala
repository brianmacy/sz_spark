package com.senzing.spark.mart

/**
 * The entity-mart target schema — four read-optimized Delta projections of the resolved entity
 * graph, plus two bookkeeping tables. This is the design's §4 schema, expressed as `CREATE TABLE`
 * DDL that works identically for a path-based OSS-Delta table (local proxy) and a Unity Catalog
 * name (Databricks) — the only difference is the table locator, which the [[EntityMartSink]]
 * supplies.
 *
 * WHY these shapes (grounded in the Senzing data-mart-replicator reference + the SDK entity-read
 * schema):
 *   - `entity` — one row per resolved entity: name, counts, representative features, hash.
 *   - `entity_record` — the ENTITY MAP: which `(data_source, record_id)` resolved into which
 *     entity, keyed by the record (one owner row per record ⇒ record-moves can't orphan).
 *   - `relationship` — entity↔entity links, normalized `lo < hi` so each pair is one row.
 *   - `entity_doc` — `entity_id → full entity JSON`, for point-lookup / serving of the document.
 *   - `_sync_state` — the sync job's watermarks (refresh_seq epoch, snapshot cursor, shard ledger).
 *   - `_quarantine` — GetCore failures (the Delta dead-letter).
 *
 * All mutation-heavy tables enable **deletion vectors** (MERGE/DELETE without rewriting whole
 * files), **Change Data Feed** (the natural downstream hook — e.g. a synced serving table), and
 * **liquid clustering** on the MERGE key (`entity_id` is high-cardinality; classic partitioning
 * would fragment the MERGE writes). `refresh_seq BIGINT` + `updated_at TIMESTAMP` bookkeeping
 * columns are everywhere so a delayed/replayed batch is a no-op (`source.refresh_seq >=
 * target.refresh_seq`).
 *
 * ⚠ Phase-1 assumption to verify on the local rig (design §10): OSS delta-spark 4.0 supports
 * `CLUSTER BY`, deletion vectors, and CDF. If any is unsupported locally, the smoke test drops the
 * corresponding clause — see [[tableProperties]] / [[clusterBy]].
 */
object EntityMartSchema {

  /** The four mart tables + two bookkeeping tables, in dependency-free creation order. */
  val Tables: Seq[String] =
    Seq("entity", "entity_record", "relationship", "entity_doc", "_sync_state", "_quarantine")

  private val Cdf = "delta.enableChangeDataFeed = true"
  private val DelVec = "delta.enableDeletionVectors = true"

  /** The column list (without the trailing bookkeeping cols, which [[bookkeeping]] appends). */
  private def columns(table: String): String = table match {
    case "entity" =>
      """entity_id      BIGINT  NOT NULL,
        |entity_name    STRING,
        |record_count   INT,
        |relation_count INT,
        |features       ARRAY<STRUCT<ftype_code: STRING, lib_feat_id: BIGINT, feat_desc: STRING,
        |                            usage_type: STRING, used_for_cand: BOOLEAN, used_for_scoring: BOOLEAN>>,
        |record_summary MAP<STRING, INT>,
        |entity_hash    STRING""".stripMargin
    case "entity_record" =>
      """data_source      STRING  NOT NULL,
        |record_id        STRING  NOT NULL,
        |entity_id        BIGINT,
        |match_key        STRING,
        |match_level_code STRING,
        |errule_code      STRING,
        |first_seen_dt    TIMESTAMP,
        |last_seen_dt     TIMESTAMP""".stripMargin
    case "relationship" =>
      """entity_id_lo     BIGINT  NOT NULL,
        |entity_id_hi     BIGINT  NOT NULL,
        |match_level_code STRING,
        |match_key        STRING,
        |rev_match_key    STRING,
        |errule_code      STRING,
        |is_disclosed     BOOLEAN,
        |is_ambiguous     BOOLEAN""".stripMargin
    case "entity_doc" =>
      """entity_id   BIGINT  NOT NULL,
        |entity_json STRING,
        |entity_hash STRING""".stripMargin
    case "_sync_state" =>
      """key         STRING  NOT NULL,
        |value        STRING""".stripMargin
    case "_quarantine" =>
      """entity_id    BIGINT,
        |category     STRING,
        |message      STRING,
        |raw          STRING""".stripMargin
    case other => throw new IllegalArgumentException(s"unknown mart table: $other")
  }

  /** The liquid-clustering clause for a table (empty for the tiny bookkeeping tables). */
  private def clusterBy(table: String): String = table match {
    case "entity" | "entity_doc" => "CLUSTER BY (entity_id)"
    case "entity_record" => "CLUSTER BY (data_source, record_id, entity_id)"
    case "relationship" => "CLUSTER BY (entity_id_lo, entity_id_hi)"
    case _ => ""
  }

  private def tableProperties(table: String): String = table match {
    case "_sync_state" | "_quarantine" => s"TBLPROPERTIES ($Cdf)"
    case _ => s"TBLPROPERTIES ($Cdf, $DelVec)"
  }

  /** The bookkeeping columns present on every mart table (not the internal tables). */
  private def bookkeeping(table: String): String = table match {
    case "_sync_state" | "_quarantine" => ""
    case _ => ",\n  refresh_seq BIGINT,\n  updated_at  TIMESTAMP"
  }

  /**
   * `CREATE TABLE IF NOT EXISTS` DDL for `table` at `locator` — a path-based identifier (``
   * delta.`/path/entity` ``) for the local proxy, or a UC 3-part name for Databricks. `USING delta`
   * is implied by the path form and explicit for the name form; callers pass the exact locator.
   */
  def createTable(table: String, locator: String): String = {
    val cols = columns(table).linesIterator.map("  " + _).mkString("\n")
    val cluster = clusterBy(table)
    val clusterLine = if (cluster.nonEmpty) s"\n$cluster" else ""
    s"""CREATE TABLE IF NOT EXISTS $locator (
       |$cols${bookkeeping(table)}
       |) USING delta$clusterLine
       |${tableProperties(table)}""".stripMargin
  }
}
