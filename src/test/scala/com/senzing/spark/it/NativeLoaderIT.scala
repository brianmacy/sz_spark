package com.senzing.spark.it

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.senzing.sdk.{SzFlag, SzRecordKey}
import com.senzing.spark.IntegrationTest
import com.senzing.spark.engine.{SzEnvGuard, SzEnvironmentFactory}
import com.senzing.spark.nativelib.NativeLibLoader
import org.scalatest.funsuite.AnyFunSuite

/**
 * The SDK initialises and resolves records WITHOUT `LD_LIBRARY_PATH`, through the native-aware
 * class loader ([[SzEnvironmentFactory.nativeAware]]) — real engine, real SQLite repository, no
 * mocks. Run by scripts/it-local.sh in its own JVM with `LD_LIBRARY_PATH` unset and the engine
 * config in `SZ_IT_ENGINE_CONFIG`; libs come from `$SENZING_DIR/er/lib`. Tagged [[IntegrationTest]]
 * (excluded from the default `sbt test`). FAILS (not skips) when its preconditions do not hold: a
 * run that cannot prove the point must not look green.
 */
final class NativeLoaderIT extends AnyFunSuite {

  private val libDir = new File(sys.env.getOrElse("SENZING_DIR", "/opt/senzing"), "er/lib")
  private def engineConfig: String =
    sys.env
      .get("SZ_IT_ENGINE_CONFIG")
      .filter(_.trim.nonEmpty)
      .getOrElse(fail("set SZ_IT_ENGINE_CONFIG"))

  test("preconditions: libSz is present and NOT reachable via LD_LIBRARY_PATH", IntegrationTest) {
    assert(new File(libDir, "libSz.so").isFile, s"no libSz.so under $libDir")
    val onPath = (sys.env.getOrElse("LD_LIBRARY_PATH", "") + File.pathSeparator +
      System.getProperty("java.library.path", "")).split(File.pathSeparator).filter(_.nonEmpty)
    assert(
      !onPath.exists(p => new File(p, "libSz.so").isFile),
      s"libSz.so must not be on LD_LIBRARY_PATH/java.library.path: ${onPath.mkString(":")}"
    )
  }

  test("without the loader, the SDK's own System.loadLibrary(\"Sz\") fails", IntegrationTest) {
    val err = intercept[UnsatisfiedLinkError](
      Class.forName("com.senzing.sdk.core.NativeEngineJni", true, getClass.getClassLoader)
    )
    assert(err.getMessage.contains("Sz"), err.getMessage)
  }

  test("with the native-aware loader the SDK initialises and resolves records", IntegrationTest) {
    NativeLibLoader.loadHelpers(libDir, dbPluginSoName = None)
    val factory = SzEnvironmentFactory.nativeAware(libDir)
    SzEnvGuard.markBuilt()
    val env = factory.build("sz-native-loader-it", engineConfig)
    try {
      assert(env.getProduct.getVersion.contains("VERSION"))
      val engine = env.getEngine
      val run = System.currentTimeMillis().toString
      val keys = Seq(s"NL-$run-1", s"NL-$run-2").map(SzRecordKey.of("TEST", _))
      keys.foreach { k =>
        engine.addRecord(
          k,
          s"""{"DATA_SOURCE":"TEST","RECORD_ID":"${k.recordId}","PRIMARY_NAME_FULL":"Native Loader $run","DATE_OF_BIRTH":"1981-02-03"}""",
          SzFlag.SZ_NO_FLAGS
        )
      }
      val mapper = new ObjectMapper()
      val ids = keys.map { k =>
        mapper
          .readTree(engine.getEntity(k, SzFlag.SZ_ENTITY_DEFAULT_FLAGS))
          .path("RESOLVED_ENTITY")
          .path("ENTITY_ID")
          .asLong()
      }
      assert(ids.head > 0 && ids.distinct.size == 1, s"expected one entity, got $ids")
    } finally {
      env.destroy()
      SzEnvGuard.markDestroyed()
    }
  }
}
