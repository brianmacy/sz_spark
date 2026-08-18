package com.senzing.spark.jobs

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{Dataset, SparkSession}

import com.senzing.spark.work._

/** Simple `key=value` job arguments. */
final case class JobArgs(
    input: String,
    outputPath: String,
    errorPath: String,
    stagingPath: String,
    partitions: Int,
    runId: String,
    redoBatch: Int,
    redoPauseMs: Long
)
object JobArgs {
  def parse(args: Array[String]): JobArgs = {
    val m = args
      .flatMap(_.split("=", 2) match {
        case Array(k, v) => Some(k -> v); case _ => None
      })
      .toMap
    JobArgs(
      input = m.getOrElse("input", ""),
      outputPath = m.getOrElse("output", "output"),
      errorPath = m.getOrElse("errors", "errors"),
      stagingPath = m.getOrElse("staging", "staging"),
      partitions = m.getOrElse("partitions", "0").toInt,
      runId = m.getOrElse("runId", "run"),
      redoBatch = m.getOrElse("redoBatch", "100000").toInt,
      redoPauseMs = m.getOrElse("redoPauseMs", "30000").toLong
    )
  }
}

/**
 * Transport glue for the data jobs: JSONL input readers. The engine-facing worker factory now lives
 * in [[com.senzing.spark.core.EngineWorker]]; this object keeps only the JSONL/args plumbing.
 */
object RecordJob {

  /**
   * JSONL where each line is a full record; BOTH `dataSource` (DATA_SOURCE) and `recordId`
   * (RECORD_ID) are parsed from the record body, and the whole line is the payload. A record
   * missing either key is NOT rejected here — it is minted with the empty key(s) and dead-lettered
   * as BadInput at the engine seam (see [[com.senzing.spark.work.RecordWorker.processOne]]).
   */
  def readRecords(spark: SparkSession, path: String): Dataset[InputRecord] = {
    import spark.implicits._
    spark.read.text(path).as[String].filter(_.trim.nonEmpty).mapPartitions { it =>
      val mapper = new ObjectMapper() // per-partition; ObjectMapper is not serializable
      it.map { line =>
        val tree = mapper.readTree(line)
        val ds = Option(tree.get("DATA_SOURCE")).map(_.asText).getOrElse("")
        val rid = Option(tree.get("RECORD_ID")).map(_.asText).getOrElse("")
        InputRecord(ds, rid, line)
      }
    }
  }

  /** Search requests: each line is an attributes JSON. */
  def readSearchRequests(spark: SparkSession, path: String): Dataset[InputRecord] = {
    import spark.implicits._
    spark.read.text(path).as[String].filter(_.trim.nonEmpty).map(line => InputRecord("", "", line))
  }
}
