package com.senzing.spark.jobs

import org.apache.spark.sql.SaveMode

import com.senzing.spark.core.RedoCore

/**
 * Scheduled, parallel redo processor. Delegates to the self-sourced [[RedoCore]] (driver-side
 * `getRedoRecord` drain → parallel `processRedoRecord` WITH_INFO), writing op=REDO affected-entity
 * rows into the same sink as the loaders. `getRedoRecord()==null` is not "done" — the queue
 * refills, so run this on a schedule.
 */
object RedoJob extends SparkJob {
  def main(args: Array[String]): Unit = {
    val a = JobArgs.parse(args)
    val spark = buildSession("sz-redo")
    try {
      val res = RedoCore.run(spark, a.runId, a.redoBatch, a.partitions)
      try {
        res.good.write.mode(SaveMode.Overwrite).parquet(a.outputPath)
        res.errors.write.mode(SaveMode.Overwrite).parquet(a.errorPath)
      } finally res.unpersist()
    } finally spark.stop()
  }
}
