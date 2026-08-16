package com.senzing.spark.glue

import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import com.senzing.spark.core.{AddCore, SplitResult}
import com.senzing.spark.jobs.SparkJob
import com.senzing.spark.work.InputRecord

/**
 * Glue Stage 2: a LONG-RUNNING Structured Streaming feeder over the parquet inbox → `core.AddCore`.
 * Uses Spark's checkpointed file source (exactly-once file feeding) rather than hand-rolled shard
 * tracking: `cleanSource=archive` replaces move-to-done, `maxFilesPerTrigger` gives backpressure. A
 * fresh JVM per invocation would re-pay native/engine/DB init every run; a persistent query
 * amortizes it and mirrors the always-on Rust fleet → fairer A/B. `Trigger.AvailableNow` is kept
 * for scheduled-batch use.
 *
 * DURABLE FAILURE CAPTURE (dead-letter): each micro-batch's `AddCore.run` returns a [[SplitResult]]
 * (affected-entity `good` frame + `errors` frame). Previously the feeder discarded it, so failed
 * records — already classified by `work.ErrorTaxonomy` and carrying
 * recordId/payload/category/errorCode/attempts — vanished into the transient overwrite-per-batch
 * `staging` dir. We now persist those frames per batch in `SaveMode.Append`:
 *   - `errors` → the `deadLetter` dir. This is the DLQ equivalent of the Rust consumer's RabbitMQ
 *     dead-letter queue / Databricks' quarantine table: the ErrorRow is self-describing enough to
 *     review AND reprocess (see [[DeadLetterReprocess]] and docs/DEAD_LETTER.md).
 *   - `good` → the `output` dir — a cheap append-only affected-entity change-notification feed.
 * Both sinks are OPTIONAL: an empty path skips the write, preserving the prior no-write behavior so
 * nothing breaks for callers that do not opt in.
 *
 * EXACTLY-ONCE NOTE: these writes live inside `foreachBatch`, which re-runs a failed batch, so a
 * shard may be written more than once. `SaveMode.Append` + idempotent downstream (dedup on
 * `(dataSource, recordId, category)` for errors, `(dataSource, recordId, entityId)` for affected)
 * is the accepted at-least-once model — the same one `addRecord` relies on. No shard is ever
 * dropped.
 *
 * Args: `inbox`, `checkpoint`, `archive`, `staging`, `maxFilesPerTrigger` (200), `trigger`
 * (`default` long-running, or `availableNow`), `deadLetter` (optional), `output` (optional).
 *
 * NO per-batch repartition: the readStream's file-partitions feed the executor slots directly, so
 * read+add_record fuse into one pipelined stage. An earlier `repartition(N)` here was pure overhead
 * — it inserted a shuffle (executor slots idle doing I/O while it ran) and, worse, could REDUCE the
 * partition count below the file count. Measured negligible on wall time (~0.1%) and it closed no
 * throughput gap; removed 2026-08-06 as dead weight.
 */
object ParquetStreamFeeder extends SparkJob {

  /**
   * Persist a micro-batch's [[SplitResult]] to the durable sinks. Both are opt-in: an empty path is
   * a no-op (back-compat). `SaveMode.Append` so each batch contributes one shard and repeated
   * batches accumulate rather than overwrite — never clobbering an earlier batch's dead-letter
   * rows. A top-level method (not a closure over the streaming query) so it is unit-testable
   * directly.
   */
  def writeSinks(result: SplitResult, deadLetter: String, output: String): Unit = {
    if (deadLetter.nonEmpty)
      result.errors
        .withColumn("failedAt", current_timestamp())
        .withColumn("source", lit("stream-inbox"))
        .write
        .mode(SaveMode.Append)
        .parquet(deadLetter)
    if (output.nonEmpty) result.good.write.mode(SaveMode.Append).parquet(output)
  }

  /**
   * Build and start the streaming query. `process` is the exactly-once per-micro-batch engine pass
   * (the `AddCore` pass in `main`); injectable — and now returning its [[SplitResult]] — so the
   * streaming/archive/checkpoint plumbing AND the dead-letter sink are testable without a real
   * engine. The returned frames are persisted via [[writeSinks]] before the next batch overwrites
   * the transient staging dir.
   */
  def run(
      spark: SparkSession,
      inbox: String,
      checkpoint: String,
      archive: String,
      maxFilesPerTrigger: Int,
      trigger: String,
      deadLetter: String,
      output: String,
      process: Dataset[InputRecord] => SplitResult
  ): StreamingQuery = {
    import spark.implicits._
    val schema = spark.emptyDataset[InputRecord].schema

    val source = spark.readStream
      .format("parquet")
      .schema(schema)
      .option("maxFilesPerTrigger", maxFilesPerTrigger)
      .option("cleanSource", "archive")
      .option("sourceArchiveDir", archive)
      .load(inbox)

    val writer = source.writeStream
      .option("checkpointLocation", checkpoint)
      .foreachBatch { (batchDf: Dataset[Row], _: Long) =>
        val input = batchDf.as[InputRecord]
        // No repartition (see class doc): file-partitions feed the slots directly; read+add fuse.
        val result = process(input)
        writeSinks(result, deadLetter, output)
        ()
      }

    val triggered =
      if (trigger == "availableNow") writer.trigger(Trigger.AvailableNow()) else writer
    triggered.start()
  }

  def main(args: Array[String]): Unit = {
    val m = GlueArgs.parse(args)
    val staging = m.getOrElse("staging", "staging")
    val runId = m.getOrElse("runId", "run")
    val spark = buildSession("sz-parquet-stream-feeder")
    try {
      val query = run(
        spark,
        inbox = m.getOrElse("inbox", ""),
        checkpoint = m.getOrElse("checkpoint", ""),
        archive = m.getOrElse("archive", ""),
        maxFilesPerTrigger = m.getOrElse("maxFilesPerTrigger", "200").toInt,
        trigger = m.getOrElse("trigger", "default"),
        deadLetter = m.getOrElse("deadLetter", ""),
        output = m.getOrElse("output", ""),
        process = ds => AddCore.run(spark, ds, runId, staging)
      )
      query.awaitTermination()
    } finally spark.stop()
  }
}
