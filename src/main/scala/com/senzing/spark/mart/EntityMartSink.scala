package com.senzing.spark.mart

import java.util.UUID

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, lit}

import com.senzing.spark.mart.EntityMartRows.{MartFrames, ParsedEntity}

/**
 * The ONE seam each deployment implements differently (design D6): engine work and row-shaping are
 * portable core; the sink is glue. `upsert` applies a batch of [[MartFrames]] idempotently; the
 * default-implemented members (`quarantine`, `readState`/`writeState`) carry the sync bookkeeping
 * so a minimal sink can ignore them. [[LocalDeltaSink]] is the OSS-`delta-spark` path-table
 * implementation; a future `DatabricksUcSink` slots in by supplying Unity-Catalog name locators
 * (see [[AbstractDeltaSink]]).
 */
trait EntityMartSink {

  /** Create the mart + bookkeeping tables if absent. */
  def initTables(): Unit

  /** Apply one batch: four keyed MERGEs + tombstone deletes, stamped with `refreshSeq`. */
  def upsert(frames: MartFrames, refreshSeq: Long): Unit

  /**
   * The "skip if unchanged" change-gate (the Senzing data-mart Entity Refresh Pattern): drop the
   * entities whose stored `entity_hash` equals the freshly-computed one, so an unchanged
   * re-resolution is a no-op across ALL four tables (the docs note many affected entities are
   * unchanged after re-resolution). Default: no gate (identity) — a sink with no readable prior
   * state treats every entity as changed. GONE tombstones bypass this (they are carried on the
   * `GetResult` stream, not the parsed set).
   */
  def selectChanged(parsed: Dataset[ParsedEntity]): Dataset[ParsedEntity] = parsed

  /** Append GetCore failures (a [[GetResult]]-shaped frame) to `_quarantine`. Default: no-op. */
  def quarantine(errors: DataFrame, refreshSeq: Long): Unit = ()

  /** Read a `_sync_state` value (e.g. the refresh watermark). Default: none. */
  def readState(key: String): Option[String] = None

  /** Upsert a `_sync_state` value. Default: no-op. */
  def writeState(key: String, value: String): Unit = ()
}

/**
 * Delta implementation shared by the local proxy and (future) Databricks glue: every operation goes
 * through `spark.sql` against a table LOCATOR, so the ONLY thing a subclass changes is how a mart
 * table is named — a path identifier (`` delta.`/path/entity` ``) for the local proxy, a UC 3-part
 * name for Databricks. The MERGE/DELETE/INSERT SQL, the `refresh_seq >= target` monotonic guard,
 * and the tombstone cascade are identical for both.
 *
 * ⚠ Requires the Delta SQL extensions on the session (`spark.sql.extensions` +
 * `spark.sql.catalog.spark_catalog`) — [[EntityMartSync]] sets them.
 */
abstract class AbstractDeltaSink(spark: SparkSession) extends EntityMartSink {

  /** The SQL locator for a mart table (path identifier or UC name). */
  protected def locator(table: String): String

  def initTables(): Unit =
    EntityMartSchema.Tables.foreach(t => spark.sql(EntityMartSchema.createTable(t, locator(t))))

  def upsert(frames: MartFrames, refreshSeq: Long): Unit = {
    mergeInto("entity", frames.entity, Seq("entity_id"), refreshSeq)
    mergeInto("entity_record", frames.entityRecord, Seq("data_source", "record_id"), refreshSeq)
    reconcileDepartedRecords(frames.entityRecord)
    // Relationship rows carry only ONE direction per refresh (this entity is the `lo` side ⇒
    // `match_key`; the `hi` side ⇒ `rev_match_key`); the OPPOSITE endpoint fills the other direction
    // on its OWN refresh. A blind `UPDATE SET *` would null whichever direction the incoming refresh
    // does not carry, so the two directions could never coexist (they would ping-pong). Column-wise
    // `coalesce(source, target)` keeps the existing value when the incoming column is null ⇒ both
    // directions accumulate. (For a column both endpoints always report — level/errule/is_* — the
    // incoming is non-null, so coalesce reduces to latest-wins, which is what we want.)
    mergeInto(
      "relationship",
      frames.relationship,
      Seq("entity_id_lo", "entity_id_hi"),
      refreshSeq,
      Some(RelationshipUpdate)
    )
    mergeInto("entity_doc", frames.entityDoc, Seq("entity_id"), refreshSeq)
    applyTombstones(frames.tombstones)
  }

  /**
   * The `relationship` MERGE's explicit `UPDATE SET` assignments: `coalesce(source, target)` per
   * business column so a single-direction refresh never nulls the opposite endpoint's direction.
   * `refresh_seq`/`updated_at` take the (newer, guard-checked) source.
   */
  private val RelationshipUpdate: Seq[String] = Seq(
    "t.match_level_code = coalesce(s.match_level_code, t.match_level_code)",
    "t.match_key = coalesce(s.match_key, t.match_key)",
    "t.rev_match_key = coalesce(s.rev_match_key, t.rev_match_key)",
    "t.errule_code = coalesce(s.errule_code, t.errule_code)",
    "t.is_disclosed = coalesce(s.is_disclosed, t.is_disclosed)",
    "t.is_ambiguous = coalesce(s.is_ambiguous, t.is_ambiguous)",
    "t.refresh_seq = s.refresh_seq",
    "t.updated_at = s.updated_at"
  )

  override def selectChanged(parsed: Dataset[ParsedEntity]): Dataset[ParsedEntity] = {
    import spark.implicits._
    val fresh = parsed.select(
      col("entity.entity_id").as("entity_id"),
      col("entity.entity_hash").as("new_hash")
    )
    val idsV = tempView("gate")
    fresh.select("entity_id").distinct().createOrReplaceTempView(idsV)
    // SELECT (unlike DELETE/UPDATE) accepts an IN-subquery, so read only the batch's stored hashes.
    val stored = spark.sql(
      s"SELECT entity_id, entity_hash AS old_hash FROM ${locator("entity")} " +
        s"WHERE entity_id IN (SELECT entity_id FROM $idsV)"
    )
    val changed = fresh
      .join(stored, Seq("entity_id"), "left")
      .where(col("old_hash").isNull || col("new_hash") =!= col("old_hash"))
      .select(col("entity_id").as("changed_id"))
      .distinct()
    val out = parsed
      .join(changed, col("entity.entity_id") === col("changed_id"), "left_semi")
      .as[ParsedEntity]
    spark.catalog.dropTempView(idsV)
    out
  }

  /**
   * Orphan-record cleanup (the Senzing data-mart orphan handling): a record DELETED from a
   * SURVIVING entity leaves a stale `entity_record` row — the upsert MERGE only touches the
   * entity's CURRENT records and never deletes an unmatched target row. For every refreshed entity
   * (those present in `freshRecords`), delete its `entity_record` rows whose
   * `(data_source, record_id)` is absent from the fresh set. A record that MOVED is safe: the
   * gaining entity's refresh re-keys it, so it IS in the fresh set — only a genuine delete departs.
   * (⚠ if the gaining entity is not in the SAME batch the row is briefly deleted and re-inserted on
   * that entity's later refresh — eventual consistency; a Phase-2 hardening would
   * `getRecord`-verify before deleting, like the reference's periodic sweep.)
   */
  private def reconcileDepartedRecords(freshRecords: DataFrame): Unit =
    if (nonEmpty(freshRecords)) {
      val idsV = tempView("reconids")
      freshRecords.select("entity_id").distinct().createOrReplaceTempView(idsV)
      val owned = spark.sql(
        s"SELECT data_source, record_id FROM ${locator("entity_record")} " +
          s"WHERE entity_id IN (SELECT entity_id FROM $idsV)"
      )
      val departed = owned.join(
        freshRecords.select("data_source", "record_id").distinct(),
        Seq("data_source", "record_id"),
        "left_anti"
      )
      if (nonEmpty(departed)) {
        val depV = tempView("recondep")
        departed.createOrReplaceTempView(depV)
        spark.sql(
          s"""MERGE INTO ${locator("entity_record")} AS t
             |USING $depV AS s
             |ON t.data_source = s.data_source AND t.record_id = s.record_id
             |WHEN MATCHED THEN DELETE""".stripMargin
        )
        spark.catalog.dropTempView(depV)
      }
      spark.catalog.dropTempView(idsV)
    }

  override def quarantine(errors: DataFrame, refreshSeq: Long): Unit =
    if (nonEmpty(errors)) {
      val v = tempView("q")
      errors
        .selectExpr("entityId AS entity_id", "category", "message", "errorCode AS raw")
        .createOrReplaceTempView(v)
      spark.sql(
        s"INSERT INTO ${locator("_quarantine")} SELECT entity_id, category, message, raw FROM $v"
      )
      spark.catalog.dropTempView(v)
    }

  override def readState(key: String): Option[String] =
    spark
      .sql(s"SELECT key, value FROM ${locator("_sync_state")}")
      .where(col("key") === key)
      .select("value")
      .take(1)
      .headOption
      .map(_.getString(0))

  override def writeState(key: String, value: String): Unit = {
    import spark.implicits._
    val v = tempView("state")
    Seq((key, value)).toDF("key", "value").createOrReplaceTempView(v)
    spark.sql(
      s"""MERGE INTO ${locator("_sync_state")} AS t
         |USING $v AS s
         |ON t.key = s.key
         |WHEN MATCHED THEN UPDATE SET t.value = s.value
         |WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)""".stripMargin
    )
    spark.catalog.dropTempView(v)
  }

  /**
   * One keyed, idempotent, refresh-monotonic MERGE: a delayed/replayed batch (`source.refresh_seq <
   * target.refresh_seq`) matches the guard and is a no-op; `INSERT *`/`UPDATE SET *` map by column
   * NAME, so the stamped source (business columns + `refresh_seq` + `updated_at`) lines up with the
   * table. Skips the MERGE job entirely when the source is empty.
   */
  private def mergeInto(
      table: String,
      source: DataFrame,
      keys: Seq[String],
      refreshSeq: Long,
      updateAssignments: Option[Seq[String]] = None
  ): Unit =
    if (nonEmpty(source)) {
      val v = tempView(table)
      stamped(source, refreshSeq).createOrReplaceTempView(v)
      val on = keys.map(k => s"t.$k = s.$k").mkString(" AND ")
      // Default: whole-row overwrite (`SET *`, INSERT/UPDATE map by column NAME). The relationship
      // table passes explicit coalesce assignments so a single-direction refresh preserves the other.
      val update = updateAssignments.map(a => s"SET ${a.mkString(", ")}").getOrElse("SET *")
      spark.sql(
        s"""MERGE INTO ${locator(table)} AS t
           |USING $v AS s
           |ON $on
           |WHEN MATCHED AND s.refresh_seq >= t.refresh_seq THEN UPDATE $update
           |WHEN NOT MATCHED THEN INSERT *""".stripMargin
      )
      spark.catalog.dropTempView(v)
    }

  /**
   * Delete every row a GONE entity still owns, across all four mart tables (the tombstone cascade).
   */
  private def applyTombstones(tombstones: DataFrame): Unit =
    if (nonEmpty(tombstones)) {
      val v = tempView("tomb")
      tombstones.select("entity_id").distinct().createOrReplaceTempView(v)
      // OSS delta-spark rejects `IN (subquery)` in DELETE (DELTA_UNSUPPORTED_SUBQUERY); a
      // delete-by-join is expressed as MERGE ... WHEN MATCHED THEN DELETE.
      def mergeDelete(table: String, on: String): Unit =
        spark.sql(
          s"""MERGE INTO ${locator(table)} AS t
             |USING $v AS s
             |ON $on
             |WHEN MATCHED THEN DELETE""".stripMargin
        )
      mergeDelete("entity", "t.entity_id = s.entity_id")
      mergeDelete("entity_doc", "t.entity_id = s.entity_id")
      mergeDelete("entity_record", "t.entity_id = s.entity_id")
      mergeDelete("relationship", "t.entity_id_lo = s.entity_id OR t.entity_id_hi = s.entity_id")
      spark.catalog.dropTempView(v)
    }

  private def stamped(df: DataFrame, refreshSeq: Long): DataFrame =
    df.withColumn("refresh_seq", lit(refreshSeq)).withColumn("updated_at", current_timestamp())

  private def nonEmpty(df: DataFrame): Boolean = df.take(1).nonEmpty

  private def tempView(tag: String): String =
    s"src_${tag}_${UUID.randomUUID().toString.replace("-", "")}"
}

/**
 * The local proxy sink: path-based OSS-Delta tables under `basePath` (e.g.
 * `/public_data/entity_mart` ⇒ `` delta.`/public_data/entity_mart/entity` ``). No Databricks
 * account required — Delta 4.x on Spark 4.0.x runs locally.
 */
final class LocalDeltaSink(spark: SparkSession, basePath: String) extends AbstractDeltaSink(spark) {
  private val base = basePath.stripSuffix("/")
  protected def locator(table: String): String = s"delta.`$base/$table`"
}
