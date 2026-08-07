package com.senzing.spark.glue

/**
 * `key=value` job-argument parsing for the glue mains, mirroring `jobs.JobArgs.parse` but returning
 * a raw map (each glue main takes a different key set, so a shared typed case class does not fit).
 */
object GlueArgs {
  def parse(args: Array[String]): Map[String, String] =
    args
      .flatMap(_.split("=", 2) match {
        case Array(k, v) => Some(k -> v); case _ => None
      })
      .toMap
}
