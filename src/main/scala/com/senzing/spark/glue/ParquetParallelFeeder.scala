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
 * `processing`, `archive` (opt), `recordsPerShard` (drainer shard size, 1000); engine: `staging`,
 * `deadLetter` (opt), `output` (opt), `recordsPerBatch` (1000), `maxUnprocessedBatches` (200),
 * `trigger` (`default`|`availableNow`), `emptyMs` (30000).
 *
 * DEFAULT OPERATING POINT: `recordsPerBatch=1000` ⇒ ONE partition/batch (independent commit,
 * straggler = one slot) and `maxUnprocessedBatches=200` ≈ slot count + buffer (so a straggler costs
 * 1 of 200 workers). Set `maxUnprocessedBatches` ≥ the cluster's `spark.cores.max`. See
 * [[OverlappingBatchEngine]] for why this is the tail-free point.
 */
object ParquetParallelFeeder extends SparkJob {

  private def buildSource(
      spark: org.apache.spark.sql.SparkSession,
      m: Map[String, String],
      recordsPerBatch: Int
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
          recordsPerBatch = recordsPerBatch,
          recordsPerShard = m.getOrElse("recordsPerShard", "1000").toInt
        )
      case other =>
        throw new IllegalArgumentException(
          s"unknown source=$other (only 'inbox' is built; kafka/delta are Step 2)"
        )
    }

  def main(args: Array[String]): Unit = {
    val m = GlueArgs.parse(args)
    val runId = m.getOrElse("runId", "run")
    val recordsPerBatch = m.getOrElse("recordsPerBatch", "1000").toInt
    val spark = buildSession(
      "sz-parallel-batch-feeder",
      extraConf = Map("spark.scheduler.mode" -> "FAIR")
    )
    try {
      val source = buildSource(spark, m, recordsPerBatch)
      OverlappingBatchEngine.run(
        spark,
        source,
        process = (ds, staging) => AddCore.run(spark, ds, runId, staging),
        stagingBase = m.getOrElse("staging", "staging"),
        deadLetter = m.getOrElse("deadLetter", ""),
        output = m.getOrElse("output", ""),
        recordsPerBatch = recordsPerBatch,
        maxUnprocessedBatches = m.getOrElse("maxUnprocessedBatches", "200").toInt,
        trigger = m.getOrElse("trigger", "default"),
        emptyMs = m.getOrElse("emptyMs", "30000").toLong
      )
    } finally spark.stop()
  }
}
