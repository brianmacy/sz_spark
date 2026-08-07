package com.senzing.spark.glue

import org.apache.hadoop.fs.Path

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
 * Args: `source` (default `inbox`; also `kafka`, `delta`), `runId`; inbox source: `inbox`,
 * `processing`, `archive` (opt), `recordsPerShard` (drainer shard size, 1000); kafka source:
 * `bootstrapServers`, `topic`, `checkpoint` (durable offset dir), `startingOffset`
 * (`earliest`|`latest`|<number>, cold start only), `minPartitions` (read fan-out, 1); delta source:
 * `tablePath`, `checkpoint`, `startingVersion` (`latest`|<number>, cold start only),
 * `versionsPerBatch` (1); engine: `staging`, `deadLetter` (opt), `output` (opt), `recordsPerBatch`
 * (1000), `maxUnprocessedBatches` (200), `trigger` (`default`|`availableNow`), `emptyMs` (30000).
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
      case "kafka" =>
        val bootstrap = m.getOrElse("bootstrapServers", "")
        val topic = m.getOrElse("topic", "")
        val checkpoint = m.getOrElse("checkpoint", "")
        require(
          bootstrap.nonEmpty && topic.nonEmpty && checkpoint.nonEmpty,
          "bootstrapServers=, topic=, and checkpoint= are required for source=kafka"
        )
        val partition = KafkaSource.SinglePartition
        // `startingOffset` (earliest|latest|<number>) is used ONLY on a cold start (no checkpoint);
        // thereafter the durable committed offset governs where reading resumes.
        val start = KafkaSource.resolveStart(
          bootstrap,
          topic,
          partition,
          m.getOrElse("startingOffset", "earliest")
        )
        val cpFile = new Path(checkpoint, s"offset-$topic-$partition")
        val watermark =
          new OffsetWatermark(ShardIo.fileSystem(spark, checkpoint), cpFile, start)
        new KafkaSource(
          spark,
          bootstrap,
          topic,
          watermark,
          recordsPerBatch = recordsPerBatch,
          minPartitions = m.getOrElse("minPartitions", "1").toInt
        )
      case "delta" =>
        val tablePath = m.getOrElse("tablePath", "")
        val checkpoint = m.getOrElse("checkpoint", "")
        require(
          tablePath.nonEmpty && checkpoint.nonEmpty,
          "tablePath= and checkpoint= are required for source=delta"
        )
        val start = DeltaSource.resolveStart(spark, tablePath, m.getOrElse("startingVersion", "0"))
        val name = tablePath.replaceAll("[^A-Za-z0-9_.-]", "_")
        val cpFile = new Path(checkpoint, s"version-$name")
        val watermark =
          new OffsetWatermark(ShardIo.fileSystem(spark, checkpoint), cpFile, start)
        new DeltaSource(
          spark,
          tablePath,
          watermark,
          versionsPerBatch = m.getOrElse("versionsPerBatch", "1").toInt
        )
      case other =>
        throw new IllegalArgumentException(
          s"unknown source=$other (built: 'inbox', 'kafka', 'delta')"
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
