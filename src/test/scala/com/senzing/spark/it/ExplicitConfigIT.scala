package com.senzing.spark.it

import com.fasterxml.jackson.databind.ObjectMapper
import com.senzing.sdk.{SzFlag, SzRecordKey}
import com.senzing.spark.IntegrationTest
import com.senzing.spark.core.AddCore
import com.senzing.spark.engine.SzEngineProvider
import com.senzing.spark.nativelib.EngineSettings
import com.senzing.spark.work.InputRecord
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

/**
 * An explicit engine config supplied on the driver reaches the executors in the task closure and is
 * used instead of `SENZING_ENGINE_CONFIGURATION_JSON` — real engine, real SQLite repository, no
 * mocks. Run by scripts/it-local.sh in its own JVM with `SENZING_ENGINE_CONFIGURATION_JSON` UNSET
 * and the config in `SZ_IT_ENGINE_CONFIG`. Tagged [[IntegrationTest]]; FAILS (not skips) when its
 * preconditions do not hold.
 */
final class ExplicitConfigIT extends AnyFunSuite {

  test("AddCore uses the explicit config carried in the task closure", IntegrationTest) {
    assert(sys.env.get(EngineSettings.EnvVar).isEmpty, s"${EngineSettings.EnvVar} must be unset")
    val config = sys.env
      .get("SZ_IT_ENGINE_CONFIG")
      .filter(_.trim.nonEmpty)
      .getOrElse(fail("set SZ_IT_ENGINE_CONFIG"))
    val run = System.currentTimeMillis().toString
    val records = Seq(s"EC-$run-1", s"EC-$run-2").map { id =>
      InputRecord(
        "TEST",
        id,
        s"""{"DATA_SOURCE":"TEST","RECORD_ID":"$id","PRIMARY_NAME_FULL":"Explicit Config $run","DATE_OF_BIRTH":"1979-04-05"}"""
      )
    }

    val spark = SparkSession
      .builder()
      .appName("explicit-config-it")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    try {
      import spark.implicits._
      val result =
        AddCore.run(spark, records.toDS().repartition(2), s"ec-$run", engineConfig = Some(config))
      try {
        assert(result.errors.count() == 0, result.errors.collect().mkString("\n"))
        assert(result.good.count() >= 1, "expected affected-entity rows")
      } finally result.unpersist()
    } finally spark.stop()

    val env = SzEngineProvider.acquire(Some(config))
    try {
      val mapper = new ObjectMapper()
      val ids = records.map { r =>
        mapper
          .readTree(
            env.getEngine
              .getEntity(SzRecordKey.of("TEST", r.recordId), SzFlag.SZ_ENTITY_DEFAULT_FLAGS)
          )
          .path("RESOLVED_ENTITY")
          .path("ENTITY_ID")
          .asLong()
      }
      assert(ids.head > 0 && ids.distinct.size == 1, s"expected one entity, got $ids")
      assertThrows[IllegalStateException](SzEngineProvider.acquire(Some("""{"SQL":{}}""")))
    } finally SzEngineProvider.release()
  }
}
