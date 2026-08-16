package com.senzing.spark.jobs

import org.apache.spark.sql.SaveMode

import com.senzing.spark.core.AddCore

/** Add/update job: reads JSONL glue, delegates to [[AddCore]], writes good/error parquet. */
object AddUpdateJob extends SparkJob {
  def main(args: Array[String]): Unit = {
    val a = JobArgs.parse(args)
    val spark = buildSession("sz-add-update")
    try {
      val input =
        randomRepartition(RecordJob.readRecords(spark, a.input), a.partitions)
      val res = AddCore.run(spark, input, a.runId)
      try {
        res.good.write.mode(SaveMode.Overwrite).parquet(a.outputPath)
        res.errors.write.mode(SaveMode.Overwrite).parquet(a.errorPath)
      } finally res.unpersist()
    } finally spark.stop()
  }
}
