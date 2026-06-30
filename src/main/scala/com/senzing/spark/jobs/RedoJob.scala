package com.senzing.spark.jobs

import org.apache.spark.sql.SaveMode

import com.senzing.spark.engine.SzEngineProvider
import com.senzing.spark.work.{InputRecord, WorkerOp}

/**
 * Scheduled, parallel redo processor. A single driver-side dequeuer pulls `getRedoRecord()` into a
 * bounded batch (RedoSource), which is repartitioned and processed in parallel via the shared
 * engine (`processRedoRecord` with WITH_INFO), emitting op=REDO affected-entity rows into the same
 * sink as the loaders. `getRedoRecord()==null` is not "done" — the queue refills, so run this on a
 * schedule.
 */
object RedoJob extends SparkJob {
  def main(args: Array[String]): Unit = {
    val a = JobArgs.parse(args)
    val spark = buildSession("sz-redo")
    try {
      // Driver-side single dequeuer (one consumer of the global queue).
      val env = SzEngineProvider.acquire()
      val engine = env.getEngine()
      val batch =
        try RedoSource.drainBatch(() => engine.getRedoRecord(), a.redoBatch)
        finally SzEngineProvider.release()

      if (batch.nonEmpty) {
        import spark.implicits._
        val input = randomRepartition(batch.toDS.map(s => InputRecord("REDO", "", s)), a.partitions)
        val res = SparkRecordOps.run(
          spark,
          input,
          a.stagingPath,
          RecordJob.engineWorkerFactory(WorkerOp.Redo, a.runId, Verbs.redo),
          acquire = () => (),
          release = () => SzEngineProvider.release()
        )
        res.good.write.mode(SaveMode.Overwrite).parquet(a.outputPath)
        res.errors.write.mode(SaveMode.Overwrite).parquet(a.errorPath)
      }
    } finally spark.stop()
  }
}
