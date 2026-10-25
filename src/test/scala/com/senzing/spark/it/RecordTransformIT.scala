package com.senzing.spark.it

import java.nio.file.Files
import java.util.concurrent.{Callable, Executors, TimeUnit}

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.senzing.sdk.{SzFlag, SzRecordKey}
import com.senzing.spark.IntegrationTest
import com.senzing.spark.engine.SzEngineProvider
import com.senzing.spark.jobs.AddUpdateJob
import com.senzing.spark.model.StagingKind
import com.senzing.spark.transform.{
  PluginTransform,
  RecordTransformException,
  RecordTransformProvider
}
import com.senzing.spark.work._
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

/**
 * The record-transform plugin hook against the REAL example plugin of sz_queue_combined_consumer
 * (`sz-record-transform-example`, merges the fields of its JSON config into every record;
 * `SZ_RT_FORCE_ERROR` in a record rejects it). No mocks. Tagged [[IntegrationTest]] (the default
 * `sbt test` excludes it); run with
 * {{{
 * (cd ~/dev/sz_queue_combined_consumer && cargo build -p sz-record-transform-example)
 * SZRT_EXAMPLE_PLUGIN=~/dev/sz_queue_combined_consumer/target/debug/libsz_record_transform_example.so \
 *   sbt 'testOnly *RecordTransformIT -- -n com.senzing.spark.IntegrationTest'
 * }}}
 * The engine test needs scripts/it-local.sh's environment (SQLite engine, SZ_IT=1) AND the provider
 * env (SENZING_RECORD_TRANSFORM_PLUGIN=$SZRT_EXAMPLE_PLUGIN,
 * SENZING_RECORD_TRANSFORM_CONFIG={"PHONE_NUMBER":"702-555-0100"}), see docs/RECORD_TRANSFORM.md.
 * Unlike the engine ITs these FAIL (not skip) when the plugin path is missing: a run that cannot
 * test the hook must not look green.
 */
final class RecordTransformIT extends AnyFunSuite {

  private val mapper = new ObjectMapper()

  private def pluginPath: String =
    sys.env.get("SZRT_EXAMPLE_PLUGIN").filter(p => new java.io.File(p).isFile).getOrElse {
      fail(
        "set SZRT_EXAMPLE_PLUGIN to the built libsz_record_transform_example.so (see this class's scaladoc)"
      )
    }

  test("empty config leaves a record unchanged (None)", IntegrationTest) {
    val t = PluginTransform.open(pluginPath, "")
    try assert(t("""{"DATA_SOURCE":"T","RECORD_ID":"1"}""").isEmpty)
    finally t.close()
  }

  test("config fields are merged into the record (Some, UTF-8 preserved)", IntegrationTest) {
    val t = PluginTransform.open(pluginPath, """{"PHONE_NUMBER":"702-555-0100"}""")
    try {
      val out = t("""{"DATA_SOURCE":"T","RECORD_ID":"1","NAME_FULL":"Zoë Müller 北京"}""")
        .getOrElse(fail("expected the plugin to replace the record"))
      val tree = mapper.readTree(out)
      assert(tree.get("PHONE_NUMBER").asText == "702-555-0100")
      assert(tree.get("NAME_FULL").asText == "Zoë Müller 北京")
    } finally t.close()
  }

  test(
    "a rejected record raises RecordTransformException with the plugin's message",
    IntegrationTest
  ) {
    val t = PluginTransform.open(pluginPath, "")
    try {
      val e = intercept[RecordTransformException](t("""{"SZ_RT_FORCE_ERROR":1}"""))
      assert(e.getMessage.contains("forced error"), e.getMessage)
    } finally t.close()
  }

  test(
    "a rejected record is dead-lettered as BAD_INPUT by the worker (no abort, next record still processed)",
    IntegrationTest
  ) {
    val t = PluginTransform.open(pluginPath, "")
    try {
      val w = new RecordWorker(
        op = WorkerOp.Add,
        runId = "r",
        verb = r =>
          t(r.payload)
            .getOrElse(r.payload)
            .replace("""{""", """{"AFFECTED_ENTITIES":[{"ENTITY_ID":1}],"""),
        counters = new Counters,
        progress = new ProgressLogger("p", 0, () => 0L, _ => ())
      )
      val bad = w.processOne(InputRecord("T", "1", """{"SZ_RT_FORCE_ERROR":1}"""))
      val good = w.processOne(InputRecord("T", "2", """{"NAME_FULL":"x"}"""))
      assert(bad.size == 1 && bad.head.kind == StagingKind.Error, "one error row")
      assert(bad.head.category.contains("BAD_INPUT"), bad.head.category.toString)
      assert(
        good.nonEmpty && good.head.kind == StagingKind.Affected,
        "the record after a rejected one is processed"
      )
    } finally t.close()
  }

  test("one handle serves concurrent threads without cross-talk", IntegrationTest) {
    val t = PluginTransform.open(pluginPath, """{"TAG":"merged"}""")
    val pool = Executors.newFixedThreadPool(16)
    try {
      val tasks = (0 until 16).map { th =>
        new Callable[Int] {
          def call(): Int = (0 until 2000).count { i =>
            val id = s"$th-$i"
            val out = t(s"""{"RECORD_ID":"$id"}""").getOrElse("")
            val tree = mapper.readTree(out)
            tree.get("RECORD_ID").asText == id && tree.get("TAG").asText == "merged"
          }
        }
      }
      val ok = pool.invokeAll(tasks.asJava).asScala.map(_.get()).sum
      assert(ok == 16 * 2000, s"$ok of ${16 * 2000} records transformed correctly")
    } finally { pool.shutdown(); pool.awaitTermination(1, TimeUnit.MINUTES); t.close() }
  }

  test(
    "a path that is not a plugin fails loudly (missing file, and a library without the symbols)",
    IntegrationTest
  ) {
    val missing =
      intercept[IllegalStateException](PluginTransform.open("/nonexistent/libnope.so", ""))
    assert(missing.getMessage.contains("/nonexistent/libnope.so"))
    val libm = Seq("/lib/x86_64-linux-gnu/libm.so.6", "/usr/lib/x86_64-linux-gnu/libm.so.6")
      .find(p => new java.io.File(p).isFile)
      .getOrElse(fail("libm.so.6 not found for the missing-symbol check"))
    val e = intercept[com.senzing.spark.transform.SzRtException](PluginTransform.open(libm, ""))
    assert(e.getMessage.contains("lacks symbol"), e.getMessage)
  }

  /** The production plugin (sz-semkey-transform): optional, needs its model directories. */
  test("semkey plugin: a person record gains the NAME_SEM_KEY feature", IntegrationTest) {
    val plugin = sys.env.get("SZRT_SEMKEY_PLUGIN")
    val config = sys.env.get("SZRT_SEMKEY_CONFIG")
    assume(
      plugin.isDefined && config.isDefined,
      "set SZRT_SEMKEY_PLUGIN and SZRT_SEMKEY_CONFIG (the --record-transform-config JSON)"
    )
    val t = PluginTransform.open(plugin.get, config.get)
    try {
      val out =
        t("""{"DATA_SOURCE":"T","RECORD_ID":"1","RECORD_TYPE":"PERSON","NAME_FULL":"Jane Q Doe"}""")
          .getOrElse(fail("the semkey plugin must rewrite a person record"))
      assert(out.contains("NAME_SEM_KEY"), s"no semantic key in: ${out.take(300)}")
    } finally t.close()
  }

  /**
   * Needs the SQLite engine (scripts/it-local.sh) and the provider env pointing at the example
   * plugin with a PHONE_NUMBER config.
   */
  test(
    "engine: AddCore loads the TRANSFORMED payload, rejects a forced error, key unchanged",
    IntegrationTest
  ) {
    assume(
      sys.env.get("SZ_IT").contains("1"),
      "requires SZ_IT=1 + the local SQLite engine (scripts/it-local.sh)"
    )
    val provider = RecordTransformProvider
      .get()
      .getOrElse(
        fail(
          "set SENZING_RECORD_TRANSFORM_PLUGIN (and SENZING_RECORD_TRANSFORM_CONFIG={\"PHONE_NUMBER\":\"702-555-0100\"})"
        )
      )
    assert(
      provider("""{"A":1}""").isDefined,
      "the configured plugin must change records (config with PHONE_NUMBER)"
    )
    val in = Files.createTempFile("rt", ".jsonl")
    Files.writeString(
      in,
      """{"DATA_SOURCE":"TEST","RECORD_ID":"RT-1","PRIMARY_NAME_FULL":"Transformed Person"}
        |{"DATA_SOURCE":"TEST","RECORD_ID":"RT-BAD","PRIMARY_NAME_FULL":"Rejected Person","SZ_RT_FORCE_ERROR":"1"}
        |""".stripMargin
    )
    def tmp(p: String): String = {
      val d = Files.createTempDirectory(p).toFile; d.delete(); d.getAbsolutePath
    }
    val (out, err) = (tmp("rtout"), tmp("rterr"))
    AddUpdateJob.main(
      Array(
        s"input=$in",
        s"output=$out",
        s"errors=$err",
        s"staging=${tmp("rtstg")}",
        "partitions=1"
      )
    )

    val engine = SzEngineProvider.engine()
    val entity = engine.getEntity(
      SzRecordKey.of("TEST", "RT-1"),
      java.util.Set.of(SzFlag.SZ_ENTITY_INCLUDE_ALL_FEATURES)
    )
    assert(
      entity.contains("702-555-0100"),
      s"the plugin's PHONE_NUMBER must be in the loaded entity: $entity"
    )
    val rejected = scala.util.Try(
      engine.getEntity(
        SzRecordKey.of("TEST", "RT-BAD"),
        java.util.Set.of(SzFlag.SZ_ENTITY_INCLUDE_ALL_FEATURES)
      )
    )
    assert(rejected.isFailure, "the rejected record must not be in the repository")

    val spark = SparkSession
      .builder()
      .appName("rt-verify")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    try {
      val errors = spark.read.parquet(err).collect()
      assert(errors.length == 1, s"exactly one dead-letter row, got ${errors.length}")
      assert(errors.head.getAs[String]("category") == "BAD_INPUT")
      assert(errors.head.getAs[String]("recordId") == "RT-BAD")
    } finally spark.stop()
  }
}
