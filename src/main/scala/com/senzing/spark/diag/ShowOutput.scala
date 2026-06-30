package com.senzing.spark.diag

import org.apache.spark.sql.SparkSession

/** Diagnostic: read an output parquet dir and print row + distinct-entity counts. */
object ShowOutput {
  def main(args: Array[String]): Unit = {
    val path = args(0)
    val spark = SparkSession.builder().appName("sz-show-output").getOrCreate()
    try {
      val df = spark.read.parquet(path)
      val rows = df.count()
      val entities = df.select("entityId").distinct().count()
      println(s"OUTPUT rows=$rows distinctEntities=$entities")
    } finally spark.stop()
  }
}
