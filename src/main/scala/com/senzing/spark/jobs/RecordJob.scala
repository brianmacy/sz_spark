package com.senzing.spark.jobs

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{Dataset, SparkSession}

import com.senzing.sdk.SzEngine
import com.senzing.spark.engine.{ConfigDrift, SzEngineProvider}
import com.senzing.spark.work._

/** Simple `key=value` job arguments. */
final case class JobArgs(
    input: String,
    outputPath: String,
    errorPath: String,
    stagingPath: String,
    dataSource: String,
    partitions: Int,
    runId: String,
    redoBatch: Int
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
      dataSource = m.getOrElse("dataSource", ""),
      partitions = m.getOrElse("partitions", "0").toInt,
      runId = m.getOrElse("runId", "run"),
      redoBatch = m.getOrElse("redoBatch", "100000").toInt
    )
  }
}

/**
 * Shared wiring for the data/redo jobs: the per-partition worker factory (binds the shared per-JVM
 * engine + config drift to an op-specific verb, with each verb under the read lock) and JSONL input
 * readers. The worker factory runs on executors.
 */
object RecordJob {

  private val ProgressEveryN = 10000L

  def engineWorkerFactory(
      op: WorkerOp,
      runId: String,
      verb: SzEngine => InputRecord => String
  ): () => RecordWorker =
    () => {
      val env = SzEngineProvider.acquire()
      val engine = env.getEngine()
      val drift = new ConfigDrift()
      val base = verb(engine)
      new RecordWorker(
        op = op,
        runId = runId,
        verb = r => SzEngineProvider.withReadLock(base(r)),
        counters = new Counters,
        progress = new ProgressLogger(
          op.tag,
          ProgressEveryN,
          () => System.currentTimeMillis(),
          msg => System.out.println(msg)
        ),
        maybeConfigDrift = () => drift.maybeReinit(env, b => SzEngineProvider.withWriteLock(b)),
        forceConfigDrift = () =>
          drift.forceCheckAndReinit(env, b => SzEngineProvider.withWriteLock(b))
      )
    }

  /**
   * JSONL where each line is a full record; `recordId` from RECORD_ID, payload is the whole line.
   */
  def readRecords(spark: SparkSession, path: String, dataSource: String): Dataset[InputRecord] = {
    import spark.implicits._
    spark.read.text(path).as[String].filter(_.trim.nonEmpty).mapPartitions { it =>
      val mapper = new ObjectMapper() // per-partition; ObjectMapper is not serializable
      it.map { line =>
        val rid = Option(mapper.readTree(line).get("RECORD_ID")).map(_.asText).getOrElse("")
        InputRecord(dataSource, rid, line)
      }
    }
  }

  /** Search requests: each line is an attributes JSON. */
  def readSearchRequests(spark: SparkSession, path: String): Dataset[InputRecord] = {
    import spark.implicits._
    spark.read.text(path).as[String].filter(_.trim.nonEmpty).map(line => InputRecord("", "", line))
  }
}
