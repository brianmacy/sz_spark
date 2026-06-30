package com.senzing.spark.jobs

import org.apache.spark.sql.SaveMode

import com.senzing.spark.engine.SzEngineProvider
import com.senzing.spark.work.WorkerOp

/** Add/update job: `addRecord` per record (WITH_INFO) → deduped affected-entity output + errors. */
object AddUpdateJob extends SparkJob {
  def main(args: Array[String]): Unit = {
    val a = JobArgs.parse(args)
    val spark = buildSession("sz-add-update")
    try {
      val input =
        randomRepartition(RecordJob.readRecords(spark, a.input, a.dataSource), a.partitions)
      val res = SparkRecordOps.run(
        spark,
        input,
        a.stagingPath,
        RecordJob.engineWorkerFactory(WorkerOp.Add, a.runId, Verbs.add),
        acquire = () => (),
        release = () => SzEngineProvider.release()
      )
      res.good.write.mode(SaveMode.Overwrite).parquet(a.outputPath)
      res.errors.write.mode(SaveMode.Overwrite).parquet(a.errorPath)
    } finally spark.stop()
  }
}
