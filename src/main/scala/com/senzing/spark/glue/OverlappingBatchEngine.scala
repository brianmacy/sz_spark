package com.senzing.spark.glue

import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, SparkSession}

import com.senzing.spark.core.SplitResult
import com.senzing.spark.work.InputRecord

/**
 * Source-agnostic OVERLAPPING-BATCH engine — the tail-killing execution model. The straggler tail
 * is fundamental and unpredictable (you can never know which records are slow), so a per-batch
 * barrier (Structured Streaming) always risks one straggler idling the cluster. This engine instead
 * keeps K chunk-jobs in flight concurrently under `spark.scheduler.mode=FAIR`, so a freed executor
 * slot immediately pulls the next pending PARTITION from ANY in-flight chunk — a slow record then
 * holds exactly ONE slot while the rest keep flowing. Partition-level work-stealing, not
 * batch-level.
 *
 * It touches ONLY the [[RecordSource]] seam, so the source can be anything (inbox / Kafka / Delta);
 * the driver does metadata (claim/commit/reclaim) + job submission, and ALL record data rides Spark
 * partitions to the executors.
 *
 * Per chunk: `df.repartition(P)` (Spark bin-packs tiny shards, this spreads them evenly across
 * slots) → ONE `process` pass (`AddCore.run`, amortizing its staging-write + sink read-backs over
 * the whole chunk, NOT per file — the fix for the v1 over-decomposition regression) → sinks once →
 * `commit`.
 *
 * At-least-once, never-drop: a chunk whose processing THROWS is NOT committed, so the source
 * reclaims it on the next restart (dispose flavor) or it is re-read from the last committed cursor
 * (watermark).
 *
 * `trigger`: `default` = long-running (poll forever); `availableNow` = drain then exit after
 * `emptyMs` idle (tests / scheduled batch).
 */
object OverlappingBatchEngine {

  private val IdlePauseMs = 200L

  /**
   * The system's own partition sizing: ~this many records per partition (the user sets
   * `recordsPerBatch`, not a partition count — the engine derives the width).
   */
  private val TargetRecordsPerPartition = 5000

  // scalastyle:off println
  private def log(msg: String): Unit = println(s"[OverlappingBatchEngine] $msg")
  private def logErr(msg: String): Unit =
    Console.err.println(s"[OverlappingBatchEngine] ERROR: $msg")
  // scalastyle:on println

  /** Outcome counts — returned for tests and logged at shutdown. */
  final case class Stats(processedChunks: Long, failedChunks: Long)

  private def sanitize(bounds: String): String = bounds.replaceAll("[^A-Za-z0-9_.-]", "_")

  def run(
      spark: SparkSession,
      source: RecordSource,
      process: (Dataset[InputRecord], String) => SplitResult,
      stagingBase: String,
      deadLetter: String,
      output: String,
      recordsPerBatch: Int,
      maxUnprocessedBatches: Int,
      trigger: String,
      emptyMs: Long
  ): Stats = {
    require(recordsPerBatch > 0, s"recordsPerBatch must be > 0, was $recordsPerBatch")
    require(
      maxUnprocessedBatches > 0,
      s"maxUnprocessedBatches must be > 0, was $maxUnprocessedBatches"
    )

    // The user sets records-per-batch + how many batches may be in flight; the engine derives the
    // partition width (the system "partitions each batch any way it wants"). K batches × P partitions
    // is the pending-partition pool a freed slot steals from.
    val concurrency = maxUnprocessedBatches
    val partitionsPerChunk = math.max(1, recordsPerBatch / TargetRecordsPerPartition)

    source.reclaim()

    val availableNow = trigger == "availableNow"
    val processed = new AtomicLong(0)
    val failed = new AtomicLong(0)
    val running = new AtomicBoolean(true)
    val lastClaimMs = new AtomicLong(System.currentTimeMillis())
    val cursor = new AtomicReference[String](source.initialCursor)

    // Serialized claim: advance the cursor atomically so watermark sources stay ordered. Claiming is
    // metadata-only, so this lock is not on the hot (per-record) path.
    def claim(): Option[Chunk] = source.synchronized {
      source.nextChunk(cursor.get()) match {
        case some @ Some(c) => cursor.set(c.nextCursor); some
        case None => None
      }
    }

    def processOne(chunk: Chunk): Unit = {
      val staging = new Path(stagingBase, sanitize(chunk.bounds)).toString
      try {
        val df = chunk.df.repartition(partitionsPerChunk)
        val result = process(df, staging)
        // Per-chunk single-file sinks (unique names) — concurrency-safe, unlike Append.
        if (deadLetter.nonEmpty) ShardIo.writeSingleFile(spark, result.errors, deadLetter, "de")
        if (output.nonEmpty) ShardIo.writeSingleFile(spark, result.good, output, "af")
        source.commit(chunk.bounds) // chunk is in the engine — dispose / advance watermark
        ShardIo.deleteQuietly(ShardIo.fileSystem(spark, staging), new Path(staging))
        val n = processed.incrementAndGet()
        log(s"committed chunk ${chunk.bounds} ($n done, ${failed.get()} failed)")
      } catch {
        case NonFatal(e) =>
          // NOT committed → source reclaims/replays it on restart. Never dropped.
          failed.incrementAndGet()
          logErr(s"chunk ${chunk.bounds} NOT committed (source will reclaim/replay on restart): $e")
      }
    }

    def workerLoop(idx: Int): Unit = {
      // Each worker's jobs go to their own FAIR pool so the K concurrent chunk-jobs share the cluster
      // fairly and a freed slot pulls the next pending partition from any of them.
      spark.sparkContext.setLocalProperty("spark.scheduler.pool", s"feeder-$idx")
      while (running.get())
        claim() match {
          case Some(c) =>
            lastClaimMs.set(System.currentTimeMillis())
            processOne(c)
          case None =>
            if (availableNow && System.currentTimeMillis() - lastClaimMs.get() >= emptyMs)
              running.set(false)
            else Thread.sleep(IdlePauseMs)
        }
    }

    log(
      s"starting: recordsPerBatch=$recordsPerBatch maxUnprocessedBatches=$maxUnprocessedBatches " +
        s"(=> $partitionsPerChunk partitions/batch, ${maxUnprocessedBatches * partitionsPerChunk} pending) trigger=$trigger"
    )
    val pool = Executors.newFixedThreadPool(concurrency)
    try {
      val workers =
        (0 until concurrency).map(i =>
          pool.submit(new Runnable { def run(): Unit = workerLoop(i) })
        )
      workers.foreach(_.get()) // blocks forever for trigger=default; until drained for availableNow
    } finally {
      pool.shutdown()
      pool.awaitTermination(1, TimeUnit.MINUTES)
    }
    val stats = Stats(processed.get(), failed.get())
    log(s"exiting: processedChunks=${stats.processedChunks} failedChunks=${stats.failedChunks}")
    stats
  }
}
