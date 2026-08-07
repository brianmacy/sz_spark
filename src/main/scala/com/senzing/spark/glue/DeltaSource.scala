package com.senzing.spark.glue

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.max

import io.delta.tables.DeltaTable

import com.senzing.spark.work.InputRecord

/**
 * Monotonic-watermark [[RecordSource]] over a Delta table's Change Data Feed — Step 2c, the
 * Databricks-native sibling of [[KafkaSource]]. Same [[OverlappingBatchEngine]], only the source
 * differs (`source=delta`). The cursor is the Delta table **version**; the durable,
 * out-of-order-safe contiguous-prefix bookkeeping reuses [[OffsetWatermark]] (a version in place of
 * an offset).
 *
 * Prerequisites: the table has **CDF enabled** (`delta.enableChangeDataFeed=true`) and a STRING
 * column **`value`** holding the JSON record body (mirroring Kafka's `value`); DATA_SOURCE /
 * RECORD_ID are projected on the executors, the whole `value` is the payload. `nextChunk` reads the
 * change feed for a bounded window of VERSIONS `[cursor, min(cursor+versionsPerBatch, latest+1))`
 * filtered to new rows; `commit` advances the watermark; `reclaim` is a no-op (restart re-reads
 * from the committed version).
 *
 * ⚠ GRANULARITY: Delta is **version-granular**, not row-count-granular — a single large commit
 * becomes one batch, so unlike [[KafkaSource]] this cannot guarantee ~`recordsPerBatch` rows per
 * batch. Keep the source table's commits modest, or raise `recordsPerBatch` so the engine
 * repartitions a big version across slots. For the tightest tail-freeness prefer the Kafka path.
 */
final class DeltaSource(
    spark: SparkSession,
    tablePath: String,
    watermark: OffsetWatermark,
    versionsPerBatch: Int
) extends RecordSource {

  require(versionsPerBatch > 0, s"versionsPerBatch must be > 0, was $versionsPerBatch")

  def initialCursor: String = watermark.committedOffset.toString

  def nextChunk(cursor: String): Option[Chunk] = {
    val start = cursor.toLong
    val latest = DeltaSource.latestVersion(spark, tablePath)
    DeltaSource.nextRange(start, latest, versionsPerBatch).map { case (s, e) =>
      // `[s, e)` exclusive cursor window ⇒ read CDF endingVersion = e-1 (inclusive).
      Chunk(bounds = s"$s-$e", df = readChanges(s, e - 1), nextCursor = e.toString)
    }
  }

  def commit(bounds: String): Unit = {
    val (s, e) = KafkaSource.parseBounds(bounds) // same "start-end" encoding as KafkaSource
    watermark.complete(s, e)
  }

  def reclaim(): Unit = () // watermark flavor: restart re-reads from the committed version

  private def readChanges(startVersion: Long, endVersion: Long): Dataset[InputRecord] = {
    import spark.implicits._
    spark.read
      .format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", startVersion)
      .option("endingVersion", endVersion)
      .load(tablePath)
      .where("_change_type IN ('insert', 'update_postimage')")
      .selectExpr(
        "COALESCE(GET_JSON_OBJECT(value, '$.DATA_SOURCE'), '') AS dataSource",
        "COALESCE(GET_JSON_OBJECT(value, '$.RECORD_ID'), '') AS recordId",
        "value AS payload"
      )
      .as[InputRecord]
  }
}

object DeltaSource {

  /**
   * The next bounded VERSION window to claim from `start` given the current `latest` (inclusive)
   * table version: `None` when caught up (`start > latest`), else `[start,
   * min(start+versionsPerBatch, latest+1))` — an exclusive-upper cursor window, so the next cursor
   * is one past the last version read.
   */
  private[glue] def nextRange(
      start: Long,
      latest: Long,
      versionsPerBatch: Int
  ): Option[(Long, Long)] =
    if (start > latest) None
    else Some((start, math.min(start + versionsPerBatch, latest + 1)))

  /** The current (max) committed version of the Delta table. */
  def latestVersion(spark: SparkSession, tablePath: String): Long =
    DeltaTable.forPath(spark, tablePath).history().agg(max("version")).head().getLong(0)

  /** Resolve a `startingVersion` spec (`latest` skips existing history; else a number). */
  def resolveStart(spark: SparkSession, tablePath: String, spec: String): Long =
    spec.trim.toLowerCase match {
      case "latest" => latestVersion(spark, tablePath) + 1 // start after the current version
      case n => n.toLong
    }
}
