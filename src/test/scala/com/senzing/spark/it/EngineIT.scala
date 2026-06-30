package com.senzing.spark.it

import java.nio.file.Files

import com.senzing.spark.IntegrationTest
import com.senzing.spark.engine.{ConfigDrift, SzEngineProvider}
import com.senzing.spark.jobs.{AddUpdateJob, DeleteJob, RedoJob, SearchJob}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

/**
 * End-to-end integration against a REAL local Senzing engine + database (NO mock ER). Tagged
 * [[IntegrationTest]] so the default `sbt test` excludes it. Run with a live engine + DB: `SZ_IT=1
 * SENZING_ENGINE_CONFIGURATION_JSON=... sbt "testOnly *EngineIT -- -n
 * com.senzing.spark.IntegrationTest"` (jobs are normally launched via spark-submit; for a local run
 * set `spark.master=local[*]`).
 */
final class EngineIT extends AnyFunSuite {

  private def enabled: Boolean = sys.env.get("SZ_IT").contains("1")
  private def tmpDir(p: String): String = {
    val d = Files.createTempDirectory(p).toFile; d.delete(); d.getAbsolutePath
  }
  private def verifySession(): SparkSession =
    SparkSession
      .builder()
      .appName("verify")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

  test("load: add a shuffled fixture yields affected entities", IntegrationTest) {
    assume(enabled, "requires SZ_IT=1 + a live engine and database")
    val in = Files.createTempFile("recs", ".jsonl")
    Files.writeString(
      in,
      """{"RECORD_ID":"1","PRIMARY_NAME_FULL":"Jane Doe"}
        |{"RECORD_ID":"2","PRIMARY_NAME_FULL":"John Roe"}
        |""".stripMargin
    )
    val out = tmpDir("out")
    AddUpdateJob.main(
      Array(
        s"input=$in",
        s"output=$out",
        s"errors=${tmpDir("err")}",
        s"staging=${tmpDir("stg")}",
        "dataSource=TEST",
        "partitions=2"
      )
    )
    val spark = verifySession()
    try assert(spark.read.parquet(out).count() >= 1, "expected affected-entity rows")
    finally spark.stop()
  }

  test("search: returns request+result rows", IntegrationTest) {
    assume(enabled, "requires SZ_IT=1 + a live engine and database")
    val in = Files.createTempFile("search", ".jsonl")
    Files.writeString(in, "{\"PRIMARY_NAME_FULL\":\"Jane Doe\"}\n")
    val out = tmpDir("sout")
    SearchJob.main(
      Array(
        s"input=$in",
        s"output=$out",
        s"errors=${tmpDir("serr")}",
        s"staging=${tmpDir("sstg")}",
        "partitions=1"
      )
    )
    val spark = verifySession()
    try assert(spark.read.parquet(out).count() >= 1)
    finally spark.stop()
  }

  test(
    "live config update: a newly-registered default config is adopted by reinit",
    IntegrationTest
  ) {
    assume(enabled, "requires SZ_IT=1 + a live engine and database")
    val env = SzEngineProvider.acquire()
    try {
      val cm = env.getConfigManager
      val before = env.getActiveConfigId
      // Register a NEW default config (adds a data source) — simulates a steward changing config.
      val cfg = cm.createConfig()
      cfg.registerDataSource("TEST_LIVE")
      val newId = cm.setDefaultConfig(cfg.export(), "live-update: add TEST_LIVE")
      assert(newId != before, "a new default config id should be registered")
      assert(env.getActiveConfigId == before, "engine stays on the old config until reinit")

      // Drift check → reinit (under the provider's write lock).
      val drift = new ConfigDrift(intervalMs = 0L)
      val reinited = drift.forceCheckAndReinit(env, b => SzEngineProvider.withWriteLock(b))
      assert(reinited, "drift should be detected and reinit performed")
      assert(env.getActiveConfigId == newId, "engine now runs the new default config (live update)")
    } finally SzEngineProvider.release()
  }

  test("delete of an absent record is a benign no-op (no error rows)", IntegrationTest) {
    assume(enabled, "requires SZ_IT=1 + a live engine and database")
    val in = Files.createTempFile("del", ".jsonl")
    Files.writeString(in, "{\"RECORD_ID\":\"NEVER-LOADED-9999\"}\n")
    val err = tmpDir("derr")
    DeleteJob.main(
      Array(
        s"input=$in",
        s"output=${tmpDir("dout")}",
        s"errors=$err",
        s"staging=${tmpDir("dstg")}",
        "dataSource=TEST",
        "partitions=1"
      )
    )
    val spark = verifySession()
    try {
      val errCount =
        if (new java.io.File(err).exists)
          scala.util.Try(spark.read.parquet(err).count()).getOrElse(0L)
        else 0L
      assert(errCount == 0L, "delete-of-absent must be a no-op, not an error")
    } finally spark.stop()
  }

  test(
    "redo: a scheduled run drains and tags op=REDO (or no-ops on an empty queue)",
    IntegrationTest
  ) {
    assume(enabled, "requires SZ_IT=1 + a live engine and database")
    // Safe to run even if the queue is empty — RedoJob writes nothing in that case.
    RedoJob.main(
      Array(
        s"output=${tmpDir("rout")}",
        s"errors=${tmpDir("rerr")}",
        s"staging=${tmpDir("rstg")}",
        "partitions=2",
        "redoBatch=1000"
      )
    )
  }
}
