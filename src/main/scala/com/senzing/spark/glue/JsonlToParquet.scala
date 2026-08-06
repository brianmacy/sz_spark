package com.senzing.spark.glue

import org.apache.spark.sql.{SaveMode, SparkSession}

import com.senzing.spark.jobs.{RecordJob, SparkJob}

/**
 * Glue: JSONL file → parquet inbox, so file loads also enter via the parquet seam. Parses BOTH
 * DATA_SOURCE and RECORD_ID from each line (reusing `RecordJob.readRecords`), then writes the
 * `InputRecord`s as parquet shards a core add job (or the streaming feeder) consumes. No engine.
 *
 * Args: `input` (JSONL path), `inbox` (parquet output dir).
 */
object JsonlToParquet extends SparkJob {

  /** Testable core: convert JSONL to parquet `InputRecord`s. Does not stop the session. */
  def run(spark: SparkSession, input: String, inbox: String): Unit =
    RecordJob
      .readRecords(spark, input)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(inbox)

  def main(args: Array[String]): Unit = {
    val m = GlueArgs.parse(args)
    val spark = buildSession("sz-jsonl-to-parquet")
    try run(spark, m.getOrElse("input", ""), m.getOrElse("inbox", ""))
    finally spark.stop()
  }
}
