package com.senzing.spark.glue

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import com.senzing.spark.jobs.SparkJob
import com.senzing.spark.work.ErrorCategory

/**
 * Reprocess path for the dead-letter dir written by [[ParquetStreamFeeder]]. This is the on-prem
 * equivalent of `MERGE`-ing a Databricks quarantine table back into the pipeline, or re-publishing
 * a RabbitMQ DLQ message: read the dead-letter Parquet, keep only the categories that can plausibly
 * succeed on a re-run, and re-emit those records as `InputRecord`-shaped Parquet shards into a
 * re-feed dir (the feeder's `inbox`, or a separate dir a supervisor then moves in). Terminal
 * categories (BAD_INPUT / NOT_FOUND) stay quarantined — re-feeding them would just loop.
 *
 * IDEMPOTENT SWEEP: each pass snapshots the shard files present now, re-emits from exactly those,
 * then ARCHIVES (renames) them out of the dead-letter dir into `archive`. So a second pass over an
 * un-changed dir re-emits nothing — the fix for the prior "re-emits every shard forever" behavior.
 * Shards the feeder writes DURING a sweep are not in the snapshot and are picked up by the next
 * pass. Re-feeding itself is safe: `add_record` is idempotent on (DATA_SOURCE, RECORD_ID), and the
 * terminal-category filter keeps BAD_INPUT / NOT_FOUND out of the re-feed loop entirely. Archived
 * shards stay Spark-readable (`spark.read.parquet(archive)`) for triage of the quarantined rows.
 *
 * A THIN HELPER, NOT A SCHEDULER: it does one pass and exits; cadence is a cron/supervisor decision
 * (see docs/DEAD_LETTER.md). Kept engine-free so it is unit-testable and never re-pays native init.
 *
 * Args: `deadLetter` (source dir), `reFeed` (destination inbox dir), `archive` (swept-shard
 * destination; defaults to `<deadLetter>-archived`).
 */
object DeadLetterReprocess extends SparkJob {

  /**
   * Categories worth re-feeding. RETRY_EXHAUSTED is a transient failure that ran out of budget;
   * CONFIG_RELEVANT / REPLACE_CONFLICT are curable once the datastore config catches up. BAD_INPUT
   * (also where NOT_FOUND lands) is terminal — a malformed/rejected payload will fail identically
   * on a re-run, so it is deliberately excluded and stays quarantined for human review.
   */
  val Reprocessable: Set[String] = Set(
    ErrorCategory.RetryExhausted.name,
    ErrorCategory.ConfigRelevant.name,
    ErrorCategory.ReplaceConflict.name
  )

  /**
   * Filter a dead-letter frame (ErrorRow / StagingRow-shaped) to the reprocessable rows and project
   * them back to the `InputRecord` columns (`dataSource`, `recordId`, `payload`). Pure — no I/O —
   * so it is directly testable.
   */
  def selectReprocessable(deadLetter: DataFrame): DataFrame =
    deadLetter
      .filter(col("category").isInCollection(Reprocessable))
      .select(col("dataSource"), col("recordId"), col("payload"))

  /**
   * Snapshot the dead-letter shards, select the reprocessable records, write them as `InputRecord`
   * Parquet shards (Append) into `reFeed` for the feeder to pick up, then archive the swept shards
   * into `archive` so the pass is idempotent. Returns the re-emitted count. A missing/empty
   * dead-letter dir is a 0-count no-op. Archiving happens LAST, after both the write and the count
   * have read the snapshot, so the source shards are still present for both actions.
   */
  def run(spark: SparkSession, deadLetter: String, reFeed: String, archive: String): Long = {
    val fs = ShardIo.fileSystem(spark, deadLetter)
    val shards = ShardIo.listShards(fs, new Path(deadLetter))
    if (shards.isEmpty) 0L
    else {
      val out = selectReprocessable(spark.read.parquet(shards.map(_.toString): _*))
      out.write.mode(SaveMode.Append).parquet(reFeed)
      val n = out.count()
      val archivePath = new Path(archive)
      shards.foreach(shard => ShardIo.dispose(fs, shard, Some(archivePath)))
      n
    }
  }

  def main(args: Array[String]): Unit = {
    val m = GlueArgs.parse(args)
    val deadLetter = m.getOrElse("deadLetter", "")
    val reFeed = m.getOrElse("reFeed", "")
    val archive = m.getOrElse("archive", s"$deadLetter-archived")
    val spark = buildSession("sz-dead-letter-reprocess")
    try {
      val n = run(spark, deadLetter, reFeed, archive)
      spark.sparkContext.setJobDescription(s"reprocessed $n dead-letter records")
      // scalastyle:off println
      println(s"DEAD_LETTER_REPROCESS re-emitted $n records to $reFeed (swept shards -> $archive)")
      // scalastyle:on println
    } finally spark.stop()
  }
}
