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
 * `entity_doc.entity_json` is a VARIANT (Spark 4 `parse_json`; Delta adds the `variantType` table
 * feature on create), so serving queries read fields by path without re-parsing a string. Every
 * table and column carries a COMMENT. A Unity Catalog sink additionally passes informational
 * PRIMARY KEY / FOREIGN KEY constraints ([[ucConstraints]]); the path-based local sink cannot (OSS
 * Spark does not parse them), so it passes none. All of CLUSTER BY, deletion vectors, CDF and
 * VARIANT are verified on OSS delta-spark 4.0 by `EntityMartSinkIT`.
 */
object EntityMartSchema {

  /**
   * The four mart tables + two bookkeeping tables, in creation order (`entity` first: FK parent).
   */
  val Tables: Seq[String] =
    Seq("entity", "entity_record", "relationship", "entity_doc", "_sync_state", "_quarantine")

  private val Cdf = "delta.enableChangeDataFeed = true"
  private val DelVec = "delta.enableDeletionVectors = true"

  /** One column: name, SQL type (with `NOT NULL` if any), and its COMMENT. */
  private final case class Col(name: String, sqlType: String, comment: String)

  private val FeatureType =
    "ARRAY<STRUCT<ftype_code: STRING, lib_feat_id: BIGINT, feat_desc: STRING, " +
      "usage_type: STRING, used_for_cand: BOOLEAN, used_for_scoring: BOOLEAN>>"

  /** The business columns (without the trailing bookkeeping columns, see [[bookkeeping]]). */
  private def columns(table: String): Seq[Col] = table match {
    case "entity" =>
      Seq(
        Col("entity_id", "BIGINT NOT NULL", "Senzing resolved entity ID"),
        Col("entity_name", "STRING", "Representative name chosen by Senzing"),
        Col("record_count", "INT", "Number of records resolved into the entity"),
        Col("relation_count", "INT", "Number of related entities"),
        Col("features", FeatureType, "Representative features (one element per feature value)"),
        Col("record_summary", "MAP<STRING, INT>", "Record count per data source"),
        Col("entity_hash", "STRING", "SHA-256 of the entity content; the skip-if-unchanged gate")
      )
    case "entity_record" =>
      Seq(
        Col("data_source", "STRING NOT NULL", "Data source code of the record"),
        Col("record_id", "STRING NOT NULL", "Record ID, unique within its data source"),
        Col("entity_id", "BIGINT", "Entity the record currently resolves to"),
        Col("match_key", "STRING", "Features that matched the record into the entity"),
        Col("match_level_code", "STRING", "How the record matched, e.g. RESOLVED"),
        Col("errule_code", "STRING", "Resolution rule that matched the record"),
        Col("first_seen_dt", "TIMESTAMP", "When the engine first saw the record"),
        Col("last_seen_dt", "TIMESTAMP", "When the engine last saw the record")
      )
    case "relationship" =>
      Seq(
        Col("entity_id_lo", "BIGINT NOT NULL", "Lower entity ID of the related pair"),
        Col("entity_id_hi", "BIGINT NOT NULL", "Higher entity ID of the related pair"),
        Col("match_level_code", "STRING", "Relationship type, e.g. POSSIBLY_SAME or DISCLOSED"),
        Col("match_key", "STRING", "Match key as reported by the lo entity"),
        Col("rev_match_key", "STRING", "Match key as reported by the hi entity"),
        Col("errule_code", "STRING", "Rule that related the pair"),
        Col("is_disclosed", "BOOLEAN", "True for a disclosed (REL_POINTER) relationship"),
        Col("is_ambiguous", "BOOLEAN", "True for an ambiguous relationship")
      )
    case "entity_doc" =>
      Seq(
        Col("entity_id", "BIGINT NOT NULL", "Senzing resolved entity ID"),
        Col("entity_json", "VARIANT", "Full getEntity response document"),
        Col("entity_hash", "STRING", "Same hash as entity.entity_hash")
      )
    case "_sync_state" =>
      Seq(
        Col("key", "STRING NOT NULL", "State key, e.g. the refresh watermark"),
        Col("value", "STRING", "State value")
      )
    case "_quarantine" =>
      Seq(
        Col("entity_id", "BIGINT", "Entity whose getEntity read failed"),
        Col("category", "STRING", "Failure category"),
        Col("message", "STRING", "Failure message"),
        Col("raw", "STRING", "Engine error code, if any")
      )
    case other => throw new IllegalArgumentException(s"unknown mart table: $other")
  }

  private def tableComment(table: String): String = table match {
    case "entity" => "Senzing entity mart: one row per resolved entity"
    case "entity_record" => "Senzing entity mart: which entity each source record resolves to"
    case "relationship" => "Senzing entity mart: entity-to-entity relationships, one row per pair"
    case "entity_doc" => "Senzing entity mart: full getEntity document per entity"
    case "_sync_state" => "Senzing entity mart: sync job watermarks"
    case "_quarantine" => "Senzing entity mart: getEntity reads that failed"
    case other => throw new IllegalArgumentException(s"unknown mart table: $other")
  }

  /** The bookkeeping columns present on every mart table (not the internal tables). */
  private def bookkeeping(table: String): Seq[Col] = table match {
    case "_sync_state" | "_quarantine" => Seq.empty
    case _ =>
      Seq(
        Col("refresh_seq", "BIGINT", "Refresh pass that last wrote the row (replay guard)"),
        Col("updated_at", "TIMESTAMP", "When the row was last written")
      )
  }

  /** Every column name of `table`, in DDL order. */
  def columnNames(table: String): Seq[String] = (columns(table) ++ bookkeeping(table)).map(_.name)

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

  /**
   * Unity Catalog informational constraints for `table` (Databricks never enforces PK/FK). The
   * primary keys are `RELY`: every write is a MERGE keyed on them, so they hold and the optimizer
   * may use them. The foreign keys are `NOT ENFORCED NORELY`: a child row can briefly name an
   * entity the mart does not have yet (`entity` is written last, and a relationship's far endpoint
   * is written by its own refresh), so the optimizer must not assume them.
   */
  def ucConstraints(table: String, locatorOf: String => String): Seq[String] = {
    def fk(name: String, col: String): String =
      s"CONSTRAINT $name FOREIGN KEY ($col) REFERENCES ${locatorOf("entity")} (entity_id) " +
        "NOT ENFORCED NORELY"
    table match {
      case "entity" => Seq("CONSTRAINT entity_pk PRIMARY KEY (entity_id) RELY")
      case "entity_record" =>
        Seq(
          "CONSTRAINT entity_record_pk PRIMARY KEY (data_source, record_id) RELY",
          fk("entity_record_entity_fk", "entity_id")
        )
      case "relationship" =>
        Seq(
          "CONSTRAINT relationship_pk PRIMARY KEY (entity_id_lo, entity_id_hi) RELY",
          fk("relationship_lo_fk", "entity_id_lo"),
          fk("relationship_hi_fk", "entity_id_hi")
        )
      case _ => Seq.empty
    }
  }

  private def sqlString(s: String): String = "'" + s.replace("'", "''") + "'"

  /**
   * `CREATE TABLE IF NOT EXISTS` DDL for `table` at `locator` — a path-based identifier (``
   * delta.`/path/entity` ``) for the local proxy, or a UC 3-part name for Databricks — plus any
   * table `constraints` (only the UC sink passes them).
   */
  def createTable(
      table: String,
      locator: String,
      constraints: Seq[String] = Seq.empty
  ): String = {
    val cols = (columns(table) ++ bookkeeping(table)).map(c =>
      s"${c.name} ${c.sqlType} COMMENT ${sqlString(c.comment)}"
    )
    val body = (cols ++ constraints).map("  " + _).mkString(",\n")
    val cluster = clusterBy(table)
    val clusterLine = if (cluster.nonEmpty) s"\n$cluster" else ""
    s"""CREATE TABLE IF NOT EXISTS $locator (
       |$body
       |) USING delta$clusterLine
       |COMMENT ${sqlString(tableComment(table))}
       |${tableProperties(table)}""".stripMargin
  }

  /**
   * Migrate, in place, an `entity_doc` created before `entity_json` became VARIANT. Delta cannot
   * change a STRING column to VARIANT, so: enable column mapping (required to drop/rename a column)
   * and the `variantType` table feature (not auto-enabled on an existing table), add a VARIANT
   * column, fill it with `parse_json`, drop the STRING column and rename the new one. Every row is
   * kept; the column moves to the end, which is harmless (the sink writes by column name). Run it
   * with the sync job stopped. Delta can refuse to read Change Data Feed across the drop/rename (a
   * non-additive schema change), so drain any pending CDF first.
   */
  def entityDocVariantMigration(locator: String): Seq[String] = {
    val comment = sqlString(columns("entity_doc").find(_.name == "entity_json").get.comment)
    Seq(
      s"ALTER TABLE $locator SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', " +
        "'delta.feature.variantType' = 'supported')",
      s"ALTER TABLE $locator ADD COLUMN entity_json_variant VARIANT COMMENT $comment",
      s"UPDATE $locator SET entity_json_variant = parse_json(entity_json)",
      s"ALTER TABLE $locator DROP COLUMN entity_json",
      s"ALTER TABLE $locator RENAME COLUMN entity_json_variant TO entity_json"
    )
  }
}
