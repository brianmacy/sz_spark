package com.senzing.spark.jobs

import org.apache.spark.sql.SaveMode

import com.senzing.spark.engine.SzEngineProvider
import com.senzing.spark.work.WorkerOp

/** Search job: `searchByAttributes` per request → (request, result) output rows + errors. */
object SearchJob extends SparkJob {
  def main(args: Array[String]): Unit = {
    val a = JobArgs.parse(args)
    val spark = buildSession("sz-search")
    try {
      val input = randomRepartition(RecordJob.readSearchRequests(spark, a.input), a.partitions)
      val res = SparkRecordOps.run(
        spark,
        input,
        a.stagingPath,
        RecordJob.engineWorkerFactory(WorkerOp.Search, a.runId, Verbs.search),
        acquire = () => (),
        release = () => SzEngineProvider.release()
      )
      res.good.write.mode(SaveMode.Overwrite).parquet(a.outputPath)
      res.errors.write.mode(SaveMode.Overwrite).parquet(a.errorPath)
    } finally spark.stop()
  }
}
