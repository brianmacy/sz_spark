package com.senzing.spark.jobs

import org.apache.spark.sql.SaveMode

import com.senzing.spark.engine.SzEngineProvider
import com.senzing.spark.work.WorkerOp

/** Delete job: `deleteRecord` per record (WITH_INFO) → deduped affected-entity output + errors. */
object DeleteJob extends SparkJob {
  def main(args: Array[String]): Unit = {
    val a = JobArgs.parse(args)
    val spark = buildSession("sz-delete")
    try {
      val input =
        randomRepartition(RecordJob.readRecords(spark, a.input, a.dataSource), a.partitions)
      val res = SparkRecordOps.run(
        spark,
        input,
        a.stagingPath,
        RecordJob.engineWorkerFactory(WorkerOp.Delete, a.runId, Verbs.delete),
        acquire = () => (),
        release = () => SzEngineProvider.release()
      )
      res.good.write.mode(SaveMode.Overwrite).parquet(a.outputPath)
      res.errors.write.mode(SaveMode.Overwrite).parquet(a.errorPath)
    } finally spark.stop()
  }
}
