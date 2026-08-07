package com.senzing.spark.glue

import com.senzing.spark.core.AddCore
import com.senzing.spark.jobs.SparkJob

/**
 * Thin main wiring a [[RecordSource]] into the source-agnostic [[OverlappingBatchEngine]] with the
 * `AddCore` engine pass — the tail-killing alternative to [[ParquetStreamFeeder]]. (Object name
 * kept for launch-script `--class` stability; the engine and source are now generic — the parquet
 * inbox is just one adapter.)
 *
 * v1 (single file per job, driver-side per-file I/O) was retired: it over-decomposed — one
 * `AddCore.run` per 5000-record shard = ~3 Spark jobs each per file — which starved the executors
 * (~4x regression). v2 hands Spark multi-file chunks and lets partitions carry the data; see
 * [[OverlappingBatchEngine]].
 *
 * Args: `source` (default `inbox`; kafka/delta are Step 2), `runId`; inbox source: `inbox`,
 * `processing`, `archive` (opt), `filesPerChunk` (B, 200); engine: `staging`, `deadLetter` (opt),
 * `output` (opt), `concurrency` (K chunks in flight, 4), `partitionsPerChunk` (P, 64), `trigger`
 * (`default`|`availableNow`), `emptyMs` (30000).
 */
object ParquetParallelFeeder extends SparkJob {

  private def buildSource(
      spark: org.apache.spark.sql.SparkSession,
      m: Map[String, String]
  ): RecordSource =
    m.getOrElse("source", "inbox") match {
      case "inbox" =>
        val inbox = m.getOrElse("inbox", "")
        val processing = m.getOrElse("processing", "")
        require(
          inbox.nonEmpty && processing.nonEmpty,
          "inbox= and processing= are required for source=inbox"
        )
        new InboxSource(
          spark,
          inbox,
          processing,
          m.getOrElse("archive", ""),
          m.getOrElse("filesPerChunk", "200").toInt
        )
      case other =>
        throw new IllegalArgumentException(
          s"unknown source=$other (only 'inbox' is built; kafka/delta are Step 2)"
        )
    }

  def main(args: Array[String]): Unit = {
    val m = GlueArgs.parse(args)
    val runId = m.getOrElse("runId", "run")
    val spark = buildSession(
      "sz-parallel-batch-feeder",
      extraConf = Map("spark.scheduler.mode" -> "FAIR")
    )
    try {
      val source = buildSource(spark, m)
      OverlappingBatchEngine.run(
        spark,
        source,
        process = (ds, staging) => AddCore.run(spark, ds, runId, staging),
        stagingBase = m.getOrElse("staging", "staging"),
        deadLetter = m.getOrElse("deadLetter", ""),
        output = m.getOrElse("output", ""),
        concurrency = m.getOrElse("concurrency", "4").toInt,
        partitionsPerChunk = m.getOrElse("partitionsPerChunk", "64").toInt,
        trigger = m.getOrElse("trigger", "default"),
        emptyMs = m.getOrElse("emptyMs", "30000").toLong
      )
    } finally spark.stop()
  }
}
