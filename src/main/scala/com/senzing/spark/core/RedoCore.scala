package com.senzing.spark.core

import org.apache.spark.sql.SparkSession

import com.senzing.spark.engine.SzEngineProvider
import com.senzing.spark.work.{InputRecord, WorkerOp}

/**
 * Core redoer: the intentional self-sourced exception. Its "source" is the engine's own
 * `SYS_EVAL_QUEUE` (`getRedoRecord`, driver-side), not an external transport, so it has no glue.
 * `drain` pulls a bounded batch on the driver (one consumer of the global queue); `process` fans a
 * NON-EMPTY batch out to the executors via the shared engine (`processRedoRecord` WITH_INFO). They
 * are split so the continuous caller can skip ALL Spark work (repartition/shuffle/empty job) when
 * the queue is currently empty.
 */
object RedoCore {

  /** Driver-side single dequeuer: drain up to `redoBatch` redo records (may return empty). */
  def drain(redoBatch: Int): Seq[String] = {
    val env = SzEngineProvider.acquire()
    val engine = env.getEngine()
    try RedoSource.drainBatch(() => engine.getRedoRecord(), redoBatch)
    finally SzEngineProvider.release()
  }

  /** Process an already-drained, NON-EMPTY batch in parallel. */
  def process(
      spark: SparkSession,
      runId: String,
      batch: Seq[String],
      partitions: Int
  ): SplitResult = {
    import spark.implicits._
    val base = batch.toDS.map(s => InputRecord("REDO", "", s))
    val input = if (partitions > 0) base.repartition(partitions) else base
    SparkRecordOps.run(
      spark,
      input,
      EngineWorker.factory(WorkerOp.Redo, runId, Verbs.redo),
      release = () => SzEngineProvider.release()
    )
  }
}
