package com.senzing.spark.diag

import com.senzing.sdk.{SzFlag, SzRecordKey}

import com.senzing.spark.engine.SzEngineProvider
import com.senzing.spark.work.InfoParser

/**
 * Engine-only self-check (no Spark) bundled in the FAT jar, so a node can verify the jar
 * self-extracts and the native engine loads + resolves with NO system Senzing install:
 * {{{
 * LD_LIBRARY_PATH=<extractDir>/lib SENZING_ENGINE_CONFIGURATION_JSON=... \
 *   java -cp sz-spark-assembly.jar com.senzing.spark.diag.SelfCheck TEST
 * }}}
 * Adds two matching records and asserts they resolve to the SAME entity (real ER), then searches.
 */
object SelfCheck {
  private val WithInfo = SzFlag.SZ_WITH_INFO_FLAGS

  def main(args: Array[String]): Unit = {
    val ds = if (args.nonEmpty) args(0) else "TEST"
    val env = SzEngineProvider.acquire()
    try {
      val engine = env.getEngine()
      val rec =
        """{"PRIMARY_NAME_FULL":"Jane Doe","DATE_OF_BIRTH":"1980-01-01","ADDR_FULL":"1 Main St, Las Vegas NV"}"""
      val e1 =
        InfoParser.affectedEntityIds(engine.addRecord(SzRecordKey.of(ds, "1001"), rec, WithInfo))
      val e2 =
        InfoParser.affectedEntityIds(engine.addRecord(SzRecordKey.of(ds, "1002"), rec, WithInfo))
      val found = InfoParser.resolvedEntityIds(
        engine.searchByAttributes("""{"PRIMARY_NAME_FULL":"Jane Doe"}""")
      )
      println(s"SELFCHECK add1=$e1 add2=$e2 search=$found")
      require(
        e1.nonEmpty && e1 == e2,
        s"two matching records must resolve to ONE entity, got $e1 vs $e2"
      )
      require(found.nonEmpty, "search must find the resolved entity")
      println("SELFCHECK OK")
    } finally SzEngineProvider.release()
  }
}
