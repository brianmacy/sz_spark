package com.senzing.spark.mart

import java.util.UUID

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

import com.senzing.spark.glue.{GlueArgs, ShardIo}
import com.senzing.spark.jobs.SparkJob
import com.senzing.spark.model.StagingKind

/**
 * Glue driver for the entity-mart refresh (design D3/D7): consume the affected-entity feed the
 * feeders already write (their `output=` dir — parquet `StagingRow` shards, `kind=AFFECTED`), dedup
 * entity ids, refresh each via [[GetCore]], shape via [[EntityMartRows]], and apply through an
 * [[EntityMartSink]]; advance a `_sync_state` watermark (`refresh_seq`, the MERGE monotonicity
 * epoch). Downstream of the feed and separate from the load feeder, so it can lag during a load
 * crunch and catch up after (the feed buffers) — the cadence is an explicit knob (design O4).
 *
 * Args: `feed` (the feeders' `output=` affected-entity dir; required), `mart` (required — a Delta
 * base PATH for `sink=local`, or a Unity Catalog `catalog.schema` for `sink=uc`), `sink` (`local` |
 * `uc`/`databricks`; default `local`), `staging` (GetCore staging root; default `<mart>/_staging`
 * for local, REQUIRED for uc — a cluster-writable DBFS/Volume dir), `trigger` (`availableNow` one
 * pass | `loop`; default `availableNow`), `cadenceMs` (loop sleep; default 60000). The two sinks
 * share ALL MERGE/DELETE logic ([[AbstractDeltaSink]]); only table naming differs. O2 =
 * pure-sz_spark target, so the feed is complete by construction (no reconciliation sweep in Phase
 * 1).
 */
object EntityMartSync extends SparkJob {

  private val StateRefreshSeq = "last_refresh_seq"

  // OSS Delta needs its SQL extensions + catalog on the session (MERGE/DELETE/CLUSTER BY/CDF). On
  // Databricks these are preset; locally, launch with `--packages io.delta:delta-spark_2.13:4.0.0`.
  private val DeltaConf = Map(
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  )

  def main(args: Array[String]): Unit = {
    val m = GlueArgs.parse(args)
    val feed = arg(m, "feed")
    val martBase = arg(m, "mart")
    val sinkKind = m.getOrElse("sink", "local")
    val staging = m.getOrElse(
      "staging",
      sinkKind match {
        case "uc" | "databricks" =>
          throw new IllegalArgumentException(
            "staging= (a cluster-writable path, e.g. a DBFS/Volume dir) is required for sink=uc"
          )
        case _ => s"${martBase.stripSuffix("/")}/_staging"
      }
    )
    val trigger = m.getOrElse("trigger", "availableNow")
    val cadenceMs = m.getOrElse("cadenceMs", "60000").toLong

    val spark = buildSession("sz-entity-mart-sync", extraConf = DeltaConf)
    try {
      // Same MERGE/DELETE logic (AbstractDeltaSink) either way — the sink only decides table NAMING:
      // a path table for the local proxy, a Unity Catalog `catalog.schema` for Databricks.
      // Engine-backed orphan-departure verifier (Phase-2): only records `getRecord` confirms gone
      // are deleted from entity_record; a moved record survives. Each call stages under a unique
      // subdir so concurrent/looped refreshes never collide.
      val verifier: DataFrame => DataFrame =
        (candidates: DataFrame) =>
          DepartedVerify.run(
            spark,
            candidates,
            s"${staging.stripSuffix("/")}/verify-${UUID.randomUUID()}"
          )
      val sink: EntityMartSink = sinkKind match {
        case "uc" | "databricks" =>
          val (catalog, schema) = DatabricksUcSink.parseTarget(martBase)
          new DatabricksUcSink(spark, catalog, schema, verifier)
        case _ => new LocalDeltaSink(spark, martBase, verifier)
      }
      sink.initTables()
      trigger match {
        case "loop" =>
          while (true) {
            runOnce(spark, feed, staging, sink)
            Thread.sleep(cadenceMs)
          }
        case _ => runOnce(spark, feed, staging, sink)
      }
    } finally spark.stop()
  }

  /**
   * One refresh pass: read the affected feed → dedup ids → GetCore → explode → sink.upsert +
   * quarantine → advance the watermark. Idempotent and refresh-monotone, so a re-run (crash /
   * replay) is safe: the MERGEs are keyed and `refresh_seq`-guarded.
   */
  def runOnce(spark: SparkSession, feed: String, staging: String, sink: EntityMartSink): Unit = {
    import spark.implicits._
    if (!feedHasShards(spark, feed)) {
      println(s"[entity-mart-sync] no feed shards under $feed — nothing to do")
      return
    }
    val refreshSeq = sink.readState(StateRefreshSeq).map(_.toLong).getOrElse(0L) + 1L
    val ids: Dataset[Long] = spark.read
      .parquet(feed)
      .where(col("kind") === StagingKind.Affected)
      .where(col("entityId").isNotNull)
      .select(col("entityId").as[Long])
      .distinct()

    val results = GetCore.run(spark, ids, s"${staging.stripSuffix("/")}/get-$refreshSeq")
    // Materialize the parsed entities ONCE (Phase-2): `parse` runs Jackson per GetResult and is
    // consumed by the change-gate join AND all four frame builds — persisting it here collapses
    // ~5 re-parses into one. In-memory (block manager), not a host-local file, per the v0.3.0 idiom.
    val parsed = EntityMartRows.parse(spark, results).persist(StorageLevel.MEMORY_AND_DISK)
    try {
      parsed.count() // force the single parse pass now
      // Change-gate: drop entities whose stored hash is unchanged, then build frames from the rest
      // (the Entity Refresh Pattern's "skip if unchanged"). Tombstones ride `results`, so GONE bypasses.
      val changed = sink.selectChanged(parsed)
      val frames = EntityMartRows.framesOf(spark, changed, results)
      sink.upsert(frames, refreshSeq)
      sink.quarantine(results.filter(_.kind == GetKind.Error).toDF(), refreshSeq)
      sink.writeState(StateRefreshSeq, refreshSeq.toString)
      println(s"[entity-mart-sync] applied refresh_seq=$refreshSeq")
    } finally parsed.unpersist(blocking = false)
  }

  private def feedHasShards(spark: SparkSession, feed: String): Boolean = {
    val fs = ShardIo.fileSystem(spark, feed)
    ShardIo.listShards(fs, new Path(feed)).nonEmpty
  }

  private def arg(m: Map[String, String], key: String): String =
    m.getOrElse(key, throw new IllegalArgumentException(s"$key= is required"))
}
