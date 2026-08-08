package com.senzing.spark.mart

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col

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
 * Args: `feed` (the feeders' `output=` affected-entity dir; required), `mart` (local Delta base
 * path; required), `staging` (GetCore staging root; default `<mart>/_staging`), `trigger`
 * (`availableNow` one pass | `loop`; default `availableNow`), `cadenceMs` (loop sleep; default
 * 60000). This is the local-proxy driver (O2 = pure-sz_spark target, so the feed is complete by
 * construction — no reconciliation sweep in Phase 1).
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
    val staging = m.getOrElse("staging", s"${martBase.stripSuffix("/")}/_staging")
    val trigger = m.getOrElse("trigger", "availableNow")
    val cadenceMs = m.getOrElse("cadenceMs", "60000").toLong

    val spark = buildSession("sz-entity-mart-sync", extraConf = DeltaConf)
    try {
      val sink = new LocalDeltaSink(spark, martBase)
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
    // Change-gate: parse, drop entities whose stored hash is unchanged, then build frames from the rest
    // (the Entity Refresh Pattern's "skip if unchanged"). Tombstones ride `results`, so GONE bypasses.
    val changed = sink.selectChanged(EntityMartRows.parse(spark, results))
    val frames = EntityMartRows.framesOf(spark, changed, results)
    sink.upsert(frames, refreshSeq)
    sink.quarantine(results.filter(_.kind == GetKind.Error).toDF(), refreshSeq)
    sink.writeState(StateRefreshSeq, refreshSeq.toString)
    println(s"[entity-mart-sync] applied refresh_seq=$refreshSeq")
  }

  private def feedHasShards(spark: SparkSession, feed: String): Boolean = {
    val fs = ShardIo.fileSystem(spark, feed)
    ShardIo.listShards(fs, new Path(feed)).nonEmpty
  }

  private def arg(m: Map[String, String], key: String): String =
    m.getOrElse(key, throw new IllegalArgumentException(s"$key= is required"))
}
