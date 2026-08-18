package com.senzing.spark.jobs

import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.spark.sql.SaveMode

import com.senzing.spark.core.RedoCore

/**
 * Continuous, parallel redo processor. One long-lived Spark session drains the engine's
 * `SYS_EVAL_QUEUE` (driver-side `getRedoRecord`) in bounded batches and fans each non-empty batch
 * out to `processRedoRecord` WITH_INFO, APPENDING op=REDO affected-entity rows (and failed-redo
 * dead-letter rows) into the same sink as the loaders. When the queue drains empty it pauses
 * `redoPauseMs` and polls again — `getRedoRecord()==null` means "empty right now", not "done": the
 * queue refills under concurrent load, so this runs until SIGTERM / container stop.
 *
 * Durability: `getRedoRecord()` is a DESTRUCTIVE dequeue, so a drained-but-unprocessed batch would
 * be lost if we stopped mid-flight. A shutdown hook sets `stopping`; the pause polls it, and we
 * only ever exit BETWEEN batches — a batch already drained is always processed before exit (the
 * residual window is one in-flight batch on hard-kill, bounded by `redoBatch`). Transient failures
 * (I/O blips, exhausted task retries) are logged and the loop continues; only a fatal error exits.
 */
object RedoJob extends SparkJob {
  @transient private lazy val log = org.slf4j.LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val a = JobArgs.parse(args)
    val spark = buildSession("sz-redo")
    val stopping = new AtomicBoolean(false)
    Runtime.getRuntime.addShutdownHook(new Thread(() => stopping.set(true)))
    try {
      while (!stopping.get() && !spark.sparkContext.isStopped) {
        try {
          val batch = RedoCore.drain(a.redoBatch)
          if (batch.isEmpty) {
            pause(a.redoPauseMs, stopping)
          } else {
            val res = RedoCore.process(spark, a.runId, batch, a.partitions)
            try {
              res.good.write.mode(SaveMode.Append).parquet(a.outputPath)
              res.errors.write.mode(SaveMode.Append).parquet(a.errorPath)
            } finally res.unpersist()
          }
        } catch {
          case NonFatal(e) =>
            log.error("redo iteration failed; continuing after pause", e)
            pause(a.redoPauseMs, stopping)
        }
      }
    } finally spark.stop()
  }

  /** Sleep up to `ms`, but wake within ~0.5s of a shutdown request so SIGTERM is responsive. */
  private def pause(ms: Long, stopping: AtomicBoolean): Unit = {
    var remaining = ms
    while (remaining > 0 && !stopping.get()) {
      val chunk = math.min(remaining, 500L)
      Thread.sleep(chunk)
      remaining -= chunk
    }
  }
}
