package com.senzing.spark.mart

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.Timestamp

import scala.jdk.CollectionConverters._
import scala.util.Try

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * PURE transform (no engine, no Delta): parse the `getEntity` documents from [[GetCore]] into the
 * four read-optimized projections of [[EntityMartSchema]] plus tombstones. The field paths are the
 * ACTUAL Senzing v4 entity-read response schema (verified against the Senzing-MCP
 * `response_schemas` for `get_entity_by_entity_id`), NOT guessed:
 *   - `RESOLVED_ENTITY.{ENTITY_ID, ENTITY_NAME, RECORD_SUMMARY[], RECORDS[], FEATURES.<FTYPE>[]}`
 *   - `RECORDS[].{DATA_SOURCE, RECORD_ID, MATCH_KEY, MATCH_LEVEL_CODE, ERRULE_CODE, FIRST/LAST_SEEN_DT}`
 *   - `RELATED_ENTITIES[].{ENTITY_ID, MATCH_LEVEL_CODE, MATCH_KEY, ERRULE_CODE, IS_AMBIGUOUS,
 *     IS_DISCLOSED}` (the two IS_* are 0/1 integers; feature `USED_FOR_CAND`/`USED_FOR_SCORING` are
 *     "Y"/"N" strings nested in `FEAT_DESC_VALUES[]`).
 *
 * The row case classes carry snake_case field names so a Spark product-encoder maps them verbatim
 * to the mart columns; the sink stamps `refresh_seq`/`updated_at`, so they are not carried here.
 *
 * The `parseEntity` / `tombstoneOf` / `entityHash` functions are Spark-free and are the unit-tested
 * correctness core ([[EntityMartRowsSpec]]); [[explode]] is the thin Spark bridge over them.
 */
object EntityMartRows {

  private val mapper = new ObjectMapper()

  // ---- row shapes (field names == mart column names) ------------------------------------------

  /**
   * One representative feature; `used_for_*` decoded from the "Y"/"N" `FEAT_DESC_VALUES` strings.
   */
  final case class FeatureRow(
      ftype_code: String,
      lib_feat_id: Option[Long],
      feat_desc: Option[String],
      usage_type: Option[String],
      used_for_cand: Option[Boolean],
      used_for_scoring: Option[Boolean]
  )

  final case class EntityRow(
      entity_id: Long,
      entity_name: Option[String],
      record_count: Option[Int],
      relation_count: Option[Int],
      features: Seq[FeatureRow],
      record_summary: Map[String, Int],
      entity_hash: String
  )

  final case class EntityRecordRow(
      data_source: String,
      record_id: String,
      entity_id: Long,
      match_key: Option[String],
      match_level_code: Option[String],
      errule_code: Option[String],
      first_seen_dt: Option[Timestamp],
      last_seen_dt: Option[Timestamp]
  )

  final case class RelationshipRow(
      entity_id_lo: Long,
      entity_id_hi: Long,
      match_level_code: Option[String],
      match_key: Option[String],
      rev_match_key: Option[String],
      errule_code: Option[String],
      is_disclosed: Option[Boolean],
      is_ambiguous: Option[Boolean]
  )

  final case class EntityDocRow(entity_id: Long, entity_json: String, entity_hash: String)

  final case class TombstoneRow(entity_id: Long)

  /**
   * All four projections derived from ONE entity document (one engine read fans out to all sinks).
   */
  final case class ParsedEntity(
      entity: EntityRow,
      records: Seq[EntityRecordRow],
      relationships: Seq[RelationshipRow],
      doc: EntityDocRow
  )

  /** The four mart frames + tombstones for one sync batch; the sink applies them idempotently. */
  final case class MartFrames(
      entity: DataFrame,
      entityRecord: DataFrame,
      relationship: DataFrame,
      entityDoc: DataFrame,
      tombstones: DataFrame
  )

  // ---- pure core (unit-tested) ----------------------------------------------------------------

  /** Parse one `getEntity` document into its four projections. Raises loudly on a malformed doc. */
  def parseEntity(entityJson: String): ParsedEntity = {
    val root = mapper.readTree(entityJson) // throws on malformed JSON — intentional
    val re = root.path("RESOLVED_ENTITY")
    val entityId = optLong(re, "ENTITY_ID").getOrElse(
      throw new IllegalArgumentException(
        "getEntity response missing RESOLVED_ENTITY.ENTITY_ID"
      )
    )
    val name = optText(re, "ENTITY_NAME")
    val summary = recordSummary(re)
    val records = entityRecords(re, entityId)
    val relationships = relatedEntities(root, entityId)
    val features = representativeFeatures(re)
    val recordCount = if (summary.nonEmpty) Some(summary.values.sum) else Some(records.size)
    val hash = entityHash(entityId, name, features, records, relationships, summary)
    val entity =
      EntityRow(entityId, name, recordCount, Some(relationships.size), features, summary, hash)
    ParsedEntity(entity, records, relationships, EntityDocRow(entityId, entityJson, hash))
  }

  /**
   * A GONE result maps to a tombstone; anything else does not. Used by [[explode]] and the spec.
   */
  def tombstoneOf(r: GetResult): Option[TombstoneRow] =
    if (r.kind == GetKind.Gone) Some(TombstoneRow(r.entityId)) else None

  private def parsedOf(r: GetResult): Option[ParsedEntity] =
    if (r.kind == GetKind.Entity) Some(parseEntity(r.json)) else None

  /**
   * A content hash for the change-gate ("skip if unchanged" — most affected entities are unchanged
   * after re-resolution). Canonical by SORTING every collection, so JSON key/element reordering
   * does NOT change it; deliberately EXCLUDES `first_seen_dt`/`last_seen_dt` (they churn on every
   * re-load of a member record without any resolution change, which would defeat the gate).
   */
  def entityHash(
      entityId: Long,
      name: Option[String],
      features: Seq[FeatureRow],
      records: Seq[EntityRecordRow],
      relationships: Seq[RelationshipRow],
      summary: Map[String, Int]
  ): String = {
    val recPart = records
      .map(r =>
        join(r.data_source, r.record_id, s(r.match_key), s(r.match_level_code), s(r.errule_code))
      )
      .sorted
    val relPart = relationships
      .map(r =>
        join(
          r.entity_id_lo.toString,
          r.entity_id_hi.toString,
          s(r.match_key),
          s(r.rev_match_key),
          s(r.match_level_code),
          s(r.errule_code),
          r.is_disclosed.getOrElse(false).toString,
          r.is_ambiguous.getOrElse(false).toString
        )
      )
      .sorted
    val featPart = features
      .map(f =>
        join(
          f.ftype_code,
          f.lib_feat_id.getOrElse(0L).toString,
          s(f.feat_desc),
          s(f.usage_type),
          f.used_for_cand.getOrElse(false).toString,
          f.used_for_scoring.getOrElse(false).toString
        )
      )
      .sorted
    val sumPart = summary.toSeq.sortBy(_._1).map { case (k, v) => join(k, v.toString) }
    val canonical =
      (Seq(join("ID", entityId.toString), join("NAME", s(name))) ++
        recPart ++ relPart ++ featPart ++ sumPart).mkString("")
    sha256(canonical)
  }

  // ---- Spark bridge ---------------------------------------------------------------------------

  /**
   * Build the four mart frames + tombstones from a tagged [[GetResult]] stream. ENTITY rows are
   * parsed and fanned out; GONE rows become tombstones; ERROR rows are ignored here (the sync
   * driver routes them to `_quarantine`).
   *
   * NOTE: `parsed` is derived lazily, so `parseEntity` re-runs once per output frame. Fine for the
   * per-micro-batch sizes here; a Phase-2 optimization would materialize `parsed` to a staging
   * write once (the SparkRecordOps read-back idiom) if the parse cost ever shows up.
   */
  def explode(spark: SparkSession, results: Dataset[GetResult]): MartFrames = {
    import spark.implicits._
    val parsed: Dataset[ParsedEntity] = results.flatMap(r => parsedOf(r).toSeq)
    MartFrames(
      entity = parsed.map(_.entity).toDF(),
      entityRecord = parsed.flatMap(_.records).toDF(),
      relationship = parsed.flatMap(_.relationships).toDF(),
      entityDoc = parsed.map(_.doc).toDF(),
      tombstones = results.flatMap(r => tombstoneOf(r).toSeq).toDF()
    )
  }

  // ---- JSON helpers ---------------------------------------------------------------------------

  private def recordSummary(re: JsonNode): Map[String, Int] = {
    val arr = re.path("RECORD_SUMMARY")
    if (!arr.isArray) Map.empty
    else
      arr
        .elements()
        .asScala
        .flatMap { n =>
          optText(n, "DATA_SOURCE").map(ds => ds -> n.path("RECORD_COUNT").asInt(0))
        }
        .toMap
  }

  private def entityRecords(re: JsonNode, entityId: Long): Seq[EntityRecordRow] = {
    val arr = re.path("RECORDS")
    if (!arr.isArray) Seq.empty
    else
      arr.elements().asScala.toSeq.map { n =>
        EntityRecordRow(
          data_source = optText(n, "DATA_SOURCE").getOrElse(""),
          record_id = optText(n, "RECORD_ID").getOrElse(""),
          entity_id = entityId,
          match_key = optText(n, "MATCH_KEY"),
          match_level_code = optText(n, "MATCH_LEVEL_CODE"),
          errule_code = optText(n, "ERRULE_CODE"),
          first_seen_dt = parseTs(optText(n, "FIRST_SEEN_DT")),
          last_seen_dt = parseTs(optText(n, "LAST_SEEN_DT"))
        )
      }
  }

  /**
   * Normalize each related entity to a `lo < hi` pair (one row per pair). When THIS entity is the
   * `lo` side, its `MATCH_KEY` is the forward direction; when it is the `hi` side, the same key is
   * the reverse direction (`rev_match_key`) — relationships can be asymmetric, and the opposite
   * endpoint's own refresh fills the missing direction.
   */
  private def relatedEntities(root: JsonNode, entityId: Long): Seq[RelationshipRow] = {
    val arr = root.path("RELATED_ENTITIES")
    if (!arr.isArray) Seq.empty
    else
      arr.elements().asScala.toSeq.flatMap { n =>
        optLong(n, "ENTITY_ID").map { relId =>
          val thisKey = optText(n, "MATCH_KEY")
          val (lo, hi, mk, rev) =
            if (entityId <= relId) (entityId, relId, thisKey, Option.empty[String])
            else (relId, entityId, Option.empty[String], thisKey)
          RelationshipRow(
            entity_id_lo = lo,
            entity_id_hi = hi,
            match_level_code = optText(n, "MATCH_LEVEL_CODE"),
            match_key = mk,
            rev_match_key = rev,
            errule_code = optText(n, "ERRULE_CODE"),
            is_disclosed = intBool(n, "IS_DISCLOSED"),
            is_ambiguous = intBool(n, "IS_AMBIGUOUS")
          )
        }
      }
  }

  private def representativeFeatures(re: JsonNode): Seq[FeatureRow] = {
    val feats = re.path("FEATURES")
    if (!feats.isObject) Seq.empty
    else
      feats.fields().asScala.toSeq.flatMap { entry =>
        val ftype = entry.getKey
        val arr = entry.getValue
        if (!arr.isArray) Seq.empty[FeatureRow]
        else
          arr.elements().asScala.toSeq.map { f =>
            val libId = optLong(f, "LIB_FEAT_ID")
            val (cand, scoring) = candScoring(f, libId)
            FeatureRow(
              ftype,
              libId,
              optText(f, "FEAT_DESC"),
              optText(f, "USAGE_TYPE"),
              cand,
              scoring
            )
          }
      }
  }

  /**
   * `USED_FOR_CAND`/`USED_FOR_SCORING` for a representative feature: present either directly on the
   * feature node or (more usually) on the matching `FEAT_DESC_VALUES[]` entry — take the direct
   * value if any, else the entry whose `LIB_FEAT_ID` matches, else the first entry.
   */
  private def candScoring(f: JsonNode, libId: Option[Long]): (Option[Boolean], Option[Boolean]) = {
    def read(n: JsonNode): (Option[Boolean], Option[Boolean]) =
      (optText(n, "USED_FOR_CAND").flatMap(yesNo), optText(n, "USED_FOR_SCORING").flatMap(yesNo))
    val direct = read(f)
    if (direct._1.isDefined || direct._2.isDefined) direct
    else {
      val vals = f.path("FEAT_DESC_VALUES")
      if (!vals.isArray || vals.size() == 0) (None, None)
      else {
        val elems = vals.elements().asScala.toSeq
        val matched = libId.flatMap(id => elems.find(v => optLong(v, "LIB_FEAT_ID").contains(id)))
        read(matched.getOrElse(elems.head))
      }
    }
  }

  private def optText(n: JsonNode, field: String): Option[String] = {
    val v = n.path(field)
    if (v.isMissingNode || v.isNull) None else Some(v.asText)
  }

  private def optLong(n: JsonNode, field: String): Option[Long] = {
    val v = n.path(field)
    if (v.isIntegralNumber) Some(v.asLong) else None
  }

  private def intBool(n: JsonNode, field: String): Option[Boolean] = {
    val v = n.path(field)
    if (v.isIntegralNumber) Some(v.asInt != 0)
    else if (v.isBoolean) Some(v.asBoolean)
    else optText(n, field).flatMap(yesNo)
  }

  private def yesNo(raw: String): Option[Boolean] = raw.trim.toUpperCase match {
    case "Y" | "YES" | "TRUE" | "1" => Some(true)
    case "N" | "NO" | "FALSE" | "0" => Some(false)
    case _ => None
  }

  private def parseTs(o: Option[String]): Option[Timestamp] =
    o.flatMap(v => Try(Timestamp.valueOf(v.trim)).toOption)

  private def s(o: Option[String]): String = o.getOrElse("")

  private def join(parts: String*): String = parts.mkString("")

  private def sha256(str: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(str.getBytes(StandardCharsets.UTF_8))
      .map(b => f"${b & 0xff}%02x")
      .mkString
}
