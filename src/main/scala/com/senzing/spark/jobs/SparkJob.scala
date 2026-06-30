package com.senzing.spark.jobs

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Shared Spark bootstrap for the data jobs. Disables speculation (engine calls are side effects
 * that must run exactly once) and provides random repartitioning (NEVER partition by a resolution
 * key — grouped records that resolve together cause lock contention).
 */
trait SparkJob {

  def buildSession(appName: String, master: Option[String] = None): SparkSession = {
    val b = SparkSession
      .builder()
      .appName(appName)
      .config("spark.speculation", "false")
    master.foreach(b.master)
    b.getOrCreate()
  }

  /** Round-robin (random) repartition to `n`. Do not repartition by name/address/zip/etc. */
  def randomRepartition[T](ds: Dataset[T], n: Int): Dataset[T] =
    if (n > 0) ds.repartition(n) else ds
}
