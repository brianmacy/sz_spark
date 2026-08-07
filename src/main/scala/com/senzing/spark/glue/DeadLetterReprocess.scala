package com.senzing.spark.glue

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
 * A THIN HELPER, NOT A SCHEDULER: it does one pass and exits. Cadence (when/how often to sweep the
 * dead-letter dir, and whether to move-or-copy) is an operational decision left to a
 * cron/supervisor — see docs/DEAD_LETTER.md. Kept engine-free so it is unit-testable and never
 * re-pays native init.
 *
 * Args: `deadLetter` (source dir), `reFeed` (destination inbox dir).
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
   * Read the dead-letter dir, select the reprocessable records, and write them as `InputRecord`
   * Parquet shards (Append) into `reFeed` for the feeder to pick up. Returns the re-emitted count.
   */
  def run(spark: SparkSession, deadLetter: String, reFeed: String): Long = {
    val src = spark.read.parquet(deadLetter)
    val out = selectReprocessable(src)
    out.write.mode(SaveMode.Append).parquet(reFeed)
    out.count()
  }

  def main(args: Array[String]): Unit = {
    val m = GlueArgs.parse(args)
    val spark = buildSession("sz-dead-letter-reprocess")
    try {
      val n = run(spark, m.getOrElse("deadLetter", ""), m.getOrElse("reFeed", ""))
      spark.sparkContext.setJobDescription(s"reprocessed $n dead-letter records")
      // scalastyle:off println
      println(s"DEAD_LETTER_REPROCESS re-emitted $n records to ${m.getOrElse("reFeed", "")}")
      // scalastyle:on println
    } finally spark.stop()
  }
}
