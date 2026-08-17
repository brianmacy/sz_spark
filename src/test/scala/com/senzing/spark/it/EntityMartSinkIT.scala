package com.senzing.spark.it

import java.nio.file.Files

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import com.senzing.spark.IntegrationTest
import com.senzing.spark.mart.EntityMartRows._
import com.senzing.spark.mart.{EntityMartRows, GetResult, LocalDeltaSink}

/**
 * Executes the real [[LocalDeltaSink]] MERGE/DELETE SQL against a local OSS delta-spark table — the
 * one thing the pure `EntityMartRowsSpec` cannot cover (that is Spark-free). Needs ONLY local Spark
 * + `delta-spark` (both on the test classpath; `delta-spark` is `Provided`), NO engine and NO
 * database — it is tagged [[IntegrationTest]] purely to keep this heavier Spark+Delta session out
 * of the fast unit `sbt test`. Run it:
 * {{{sbt "testOnly *EntityMartSinkIT -- -n com.senzing.spark.IntegrationTest"}}}
 *
 * The headline case is the relationship direction-accumulation the column-wise `coalesce` MERGE
 * fixes: entity 100's refresh fills the FORWARD `match_key` of pair (100,200); entity 200's later
 * refresh fills the REVERSE `rev_match_key` of the SAME pair. A blind `UPDATE SET *` would null the
 * forward key on 200's refresh; `coalesce` keeps both. Also asserts the `refresh_seq` monotonic
 * guard (a stale replay is a no-op) and the tombstone cascade.
 */
final class EntityMartSinkIT extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var base: String = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("sz-emart-sink-it")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    base = Files.createTempDirectory("sz-emart-it").toString
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  /** A [[MartFrames]] with only the relationship frame populated (the others skip as empty). */
  private def relFrames(rows: RelationshipRow*): MartFrames = {
    val spk = spark // stable identifier — implicits import needs a val, not the `spark` var
    import spk.implicits._
    MartFrames(
      entity = Seq.empty[EntityRow].toDF(),
      entityRecord = Seq.empty[EntityRecordRow].toDF(),
      relationship = rows.toSeq.toDF(),
      entityDoc = Seq.empty[EntityDocRow].toDF(),
      tombstones = Seq.empty[TombstoneRow].toDF()
    )
  }

  private def rel(
      lo: Long,
      hi: Long,
      level: String,
      matchKey: Option[String],
      revKey: Option[String]
  ): RelationshipRow =
    RelationshipRow(lo, hi, Some(level), matchKey, revKey, Some("CFF"), Some(false), Some(false))

  private def relationshipTable(): DataFrame =
    spark.sql(
      s"SELECT * FROM delta.`$base/relationship` WHERE entity_id_lo = 100 AND entity_id_hi = 200"
    )

  test(
    "relationship: both directions accumulate across the two endpoints' refreshes",
    IntegrationTest
  ) {
    val sink = new LocalDeltaSink(spark, base)
    sink.initTables() // exercises the CLUSTER BY + deletion-vector + CDF DDL on OSS delta-spark

    // Entity 100 refreshes first: it is the `lo` side of (100,200) ⇒ forward match_key only.
    sink.upsert(
      relFrames(rel(100, 200, "POSSIBLY_RELATED", Some("+ADDRESS"), None)),
      refreshSeq = 1
    )
    val afterA = relationshipTable().collect()
    assert(afterA.length == 1, "exactly one (100,200) row after the first refresh")
    assert(afterA.head.getAs[String]("match_key") == "+ADDRESS")
    assert(afterA.head.getAs[String]("rev_match_key") == null, "reverse not filled yet")

    // Entity 200 refreshes later: it is the `hi` side of (100,200) ⇒ reverse rev_match_key only. A
    // blind UPDATE SET * would null match_key here; the coalesce MERGE must keep BOTH.
    sink.upsert(relFrames(rel(100, 200, "POSSIBLY_RELATED", None, Some("+NAME"))), refreshSeq = 2)
    val afterB = relationshipTable().collect()
    assert(afterB.length == 1, "still one row (same pair, not a duplicate)")
    assert(afterB.head.getAs[String]("match_key") == "+ADDRESS", "forward direction preserved")
    assert(afterB.head.getAs[String]("rev_match_key") == "+NAME", "reverse direction filled")
    assert(afterB.head.getAs[Long]("refresh_seq") == 2L)
  }

  test("relationship: a stale (lower refresh_seq) replay is a no-op", IntegrationTest) {
    val sink = new LocalDeltaSink(spark, base)
    // Table already at refresh_seq=2 from the prior test. A replay at seq=1 with a changed level must
    // be rejected by the `s.refresh_seq >= t.refresh_seq` guard.
    sink.upsert(relFrames(rel(100, 200, "DISCLOSED", Some("+ZZZ"), Some("+ZZZ"))), refreshSeq = 1)
    val row = relationshipTable().collect().head
    assert(row.getAs[String]("match_level_code") == "POSSIBLY_RELATED", "stale level rejected")
    assert(row.getAs[String]("match_key") == "+ADDRESS", "stale key rejected")
    assert(row.getAs[Long]("refresh_seq") == 2L)
  }

  test("tombstone cascade deletes the GONE entity's relationship rows", IntegrationTest) {
    val spk = spark // stable identifier — implicits import needs a val, not the `spark` var
    import spk.implicits._
    val sink = new LocalDeltaSink(spark, base)
    // Entity 200 goes GONE ⇒ the (100,200) row (200 on the hi side) must be deleted.
    val frames = MartFrames(
      entity = Seq.empty[EntityRow].toDF(),
      entityRecord = Seq.empty[EntityRecordRow].toDF(),
      relationship = Seq.empty[RelationshipRow].toDF(),
      entityDoc = Seq.empty[EntityDocRow].toDF(),
      tombstones = Seq(TombstoneRow(200L)).toDF()
    )
    sink.upsert(frames, refreshSeq = 3)
    assert(relationshipTable().collect().isEmpty, "GONE entity's relationship row cascaded away")
  }

  /**
   * A [[ParsedEntity]] with a chosen hash and record set (no relationships) — change-gate/orphan
   * input.
   */
  private def parsed(id: Long, hash: String, recs: Seq[(String, String)]): ParsedEntity = {
    val records = recs.map { case (ds, rid) =>
      EntityRecordRow(ds, rid, id, None, None, None, None, None)
    }
    ParsedEntity(
      EntityRow(id, Some(s"E$id"), Some(records.size), Some(0), Seq.empty, Map.empty, hash),
      records,
      Seq.empty,
      EntityDocRow(id, s"""{"id":$id}""", hash)
    )
  }

  /** Build frames from parsed entities (no tombstones) and apply them through the real sink. */
  private def applyParsed(sink: LocalDeltaSink, refreshSeq: Long, ps: ParsedEntity*): Unit = {
    val spk = spark
    import spk.implicits._
    val frames = EntityMartRows.framesOf(spk, ps.toSeq.toDS(), spk.emptyDataset[GetResult])
    sink.upsert(frames, refreshSeq)
  }

  private def recordIds(entityId: Long): Set[String] =
    spark
      .sql(s"SELECT record_id FROM delta.`$base/entity_record` WHERE entity_id = $entityId")
      .collect()
      .map(_.getString(0))
      .toSet

  test(
    "change-gate: an unchanged entity is skipped; a changed / new entity passes",
    IntegrationTest
  ) {
    val spk = spark
    import spk.implicits._
    val sink = new LocalDeltaSink(spark, base)
    sink.initTables()
    applyParsed(sink, refreshSeq = 10, parsed(1L, "H1", Seq(("DSg", "R1")))) // seed entity 1 @ H1

    // Same entity + same hash ⇒ gated OUT (unchanged).
    assert(sink.selectChanged(Seq(parsed(1L, "H1", Seq(("DSg", "R1")))).toDS()).isEmpty)
    // Same entity, NEW hash ⇒ passes (content changed).
    assert(sink.selectChanged(Seq(parsed(1L, "H2", Seq(("DSg", "R1")))).toDS()).count() == 1L)
    // Brand-new entity (no stored hash) ⇒ passes.
    assert(sink.selectChanged(Seq(parsed(999L, "Hx", Seq.empty)).toDS()).count() == 1L)
  }

  test(
    "orphan record: a record deleted from a surviving entity is reconciled away",
    IntegrationTest
  ) {
    val sink = new LocalDeltaSink(spark, base)
    sink.initTables()
    applyParsed(sink, refreshSeq = 20, parsed(2L, "Ha", Seq(("DSo", "A1"), ("DSo", "A2"))))
    assert(recordIds(2L) == Set("A1", "A2"), "both records present after first refresh")

    // Entity 2 refreshes with only A1 (A2 deleted from the entity) ⇒ A2's stale row must be removed.
    applyParsed(sink, refreshSeq = 21, parsed(2L, "Hb", Seq(("DSo", "A1"))))
    assert(recordIds(2L) == Set("A1"), "departed record A2 reconciled away")
  }

  test(
    "orphan verify (Phase-2): only verifier-confirmed-gone records are deleted; moved records survive",
    IntegrationTest
  ) {
    // Engine-backed verifier stand-in: A2 is genuinely gone, A3 merely MOVED (still exists) ⇒ the
    // verifier returns only A2, so A3 must NOT be reconciled away (it is re-keyed by its gaining
    // entity's own refresh). Proves the DepartedVerify seam short-circuits the Phase-1 delete.
    val verifier: DataFrame => DataFrame = df => df.filter("record_id = 'A2'")
    val sink = new LocalDeltaSink(spark, base, verifier)
    sink.initTables()
    applyParsed(
      sink,
      refreshSeq = 30,
      parsed(3L, "Hc", Seq(("DSo", "A1"), ("DSo", "A2"), ("DSo", "A3")))
    )
    assert(recordIds(3L) == Set("A1", "A2", "A3"), "all three records present after first refresh")

    // Entity 3 refreshes with only A1 ⇒ A2 and A3 both depart the fresh set; the verifier confirms
    // only A2 gone.
    applyParsed(sink, refreshSeq = 31, parsed(3L, "Hd", Seq(("DSo", "A1"))))
    assert(recordIds(3L) == Set("A1", "A3"), "A2 (verified gone) deleted; A3 (moved) survived")
  }
}
