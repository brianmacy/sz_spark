package com.senzing.spark.jobs

import org.apache.spark.sql.SaveMode

import com.senzing.spark.core.SearchCore

/** Search job: reads request-JSONL glue, delegates to [[SearchCore]], writes good/error parquet. */
object SearchJob extends SparkJob {
  def main(args: Array[String]): Unit = {
    val a = JobArgs.parse(args)
    val spark = buildSession("sz-search")
    try {
      val input = randomRepartition(RecordJob.readSearchRequests(spark, a.input), a.partitions)
      val res = SearchCore.run(spark, input, a.runId, a.stagingPath)
      res.good.write.mode(SaveMode.Overwrite).parquet(a.outputPath)
      res.errors.write.mode(SaveMode.Overwrite).parquet(a.errorPath)
    } finally spark.stop()
  }
}
