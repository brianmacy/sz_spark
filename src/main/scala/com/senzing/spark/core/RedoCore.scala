package com.senzing.spark.core

import org.apache.spark.sql.SparkSession

import com.senzing.spark.engine.SzEngineProvider
import com.senzing.spark.work.{InputRecord, WorkerOp}

/**
 * Core redoer: the intentional self-sourced exception. Its "source" is the engine's own
 * `SYS_EVAL_QUEUE` (`getRedoRecord`, driver-side), not an external transport, so it has no glue. A
 * single driver-side dequeuer pulls a bounded batch (RedoSource), which is repartitioned and
 * processed in parallel via the shared engine (`processRedoRecord` WITH_INFO). An empty batch
 * yields empty good/error frames (SparkRecordOps on an empty Dataset).
 */
object RedoCore {
  def run(
      spark: SparkSession,
      runId: String,
      stagingPath: String,
      redoBatch: Int,
      partitions: Int
  ): SplitResult = {
    import spark.implicits._

    // Driver-side single dequeuer (one consumer of the global queue).
    val env = SzEngineProvider.acquire()
    val engine = env.getEngine()
    val batch =
      try RedoSource.drainBatch(() => engine.getRedoRecord(), redoBatch)
      finally SzEngineProvider.release()

    val base = batch.toDS.map(s => InputRecord("REDO", "", s))
    val input = if (partitions > 0) base.repartition(partitions) else base

    SparkRecordOps.run(
      spark,
      input,
      stagingPath,
      EngineWorker.factory(WorkerOp.Redo, runId, Verbs.redo),
      release = () => SzEngineProvider.release()
    )
  }
}
