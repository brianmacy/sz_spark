package com.senzing.spark.diag

import com.senzing.sdk.{SzFlag, SzRecordKey}

import com.senzing.spark.engine.SzEngineProvider
import com.senzing.spark.work.InfoParser

/**
 * Diagnostic: observe what `deleteRecord` does for a never-loaded record id, to finalize the
 * worker's NOT_FOUND disposition (the SDK signature declares only
 * SzUnknownDataSourceException/SzException).
 */
object DeleteProbe {
  def main(args: Array[String]): Unit = {
    val ds = if (args.nonEmpty) args(0) else "TEST"
    val env = SzEngineProvider.acquire()
    try {
      val engine = env.getEngine()
      try {
        val info =
          engine.deleteRecord(SzRecordKey.of(ds, "NEVER-LOADED-9999"), SzFlag.SZ_WITH_INFO_FLAGS)
        println(s"DELETE_ABSENT: returned normally; affected=${InfoParser.affectedEntityIds(info)}")
      } catch {
        case e: Throwable => println(s"DELETE_ABSENT: threw ${e.getClass.getName}: ${e.getMessage}")
      }
    } finally SzEngineProvider.release()
  }
}
