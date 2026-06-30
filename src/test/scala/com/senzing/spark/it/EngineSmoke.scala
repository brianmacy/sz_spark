package com.senzing.spark.it

import com.senzing.sdk.{SzFlag, SzRecordKey}

import com.senzing.spark.engine.SzEngineProvider
import com.senzing.spark.work.InfoParser

/**
 * Minimal REAL-engine smoke (no Spark): drives the actual SzEngineProvider build +
 * addRecord/search/ redo against a live engine, parsing with the real InfoParser. Run with a
 * configured engine:
 * {{{
 * LD_LIBRARY_PATH=/opt/senzing/er/lib \
 * SENZING_ENGINE_CONFIGURATION_JSON='{...sqlite...}' \
 * sbt "Test/runMain com.senzing.spark.it.EngineSmoke"
 * }}}
 */
object EngineSmoke {
  private val WithInfo = SzFlag.SZ_WITH_INFO_FLAGS

  def main(args: Array[String]): Unit = {
    val ds = if (args.nonEmpty) args(0) else "TEST"
    val env = SzEngineProvider.acquire()
    try {
      val engine = env.getEngine()

      val a1 = engine.addRecord(
        SzRecordKey.of(ds, "1001"),
        """{"PRIMARY_NAME_FULL":"Jane Doe","DATE_OF_BIRTH":"1980-01-01","ADDR_FULL":"1 Main St, Las Vegas NV"}""",
        WithInfo
      )
      println("ADD 1001 -> AFFECTED_ENTITIES=" + InfoParser.affectedEntityIds(a1))

      val a2 = engine.addRecord(
        SzRecordKey.of(ds, "1002"),
        """{"PRIMARY_NAME_FULL":"Jane Doe","DATE_OF_BIRTH":"1980-01-01","ADDR_FULL":"1 Main Street, Las Vegas Nevada"}""",
        WithInfo
      )
      println("ADD 1002 -> AFFECTED_ENTITIES=" + InfoParser.affectedEntityIds(a2))

      val s = engine.searchByAttributes("""{"PRIMARY_NAME_FULL":"Jane Doe"}""")
      println("SEARCH 'Jane Doe' -> RESOLVED_ENTITIES=" + InfoParser.resolvedEntityIds(s))

      var processed = 0
      var r = engine.getRedoRecord()
      while (r != null && r.nonEmpty && processed < 1000) {
        engine.processRedoRecord(r, WithInfo)
        processed += 1
        r = engine.getRedoRecord()
      }
      println(s"REDO drained -> processed=$processed")
      println("SMOKE OK")
    } finally SzEngineProvider.release()
  }
}
