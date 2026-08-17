package com.senzing.spark.mart

import org.apache.spark.TaskContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import com.senzing.sdk.{SzEngine, SzFlag, SzNotFoundException, SzRecordKey}
import com.senzing.spark.engine.SzEngineProvider
import com.senzing.spark.work.{ErrorCategory, ErrorTaxonomy}

/**
 * Orphan-departure verifier — the Phase-2 hardening of
 * [[AbstractDeltaSink.reconcileDepartedRecords]]. A record that leaves a SURVIVING entity's fresh
 * record set is a delete candidate, but it may merely have MOVED to an entity not refreshed in this
 * batch; deleting it then would briefly drop a still-live record until the gaining entity's refresh
 * re-keys it (the eventual-consistency window the Phase-1 code accepted). This bracket asks the
 * engine `getRecord` for each candidate and keeps ONLY the records that are genuinely gone
 * (`SzNotFoundException`); a record that still exists (moved) is dropped from the delete set and
 * left for the gaining entity's own refresh to re-key.
 *
 * Existence-only read (`SZ_NO_FLAGS`). The engine bracket + single-committed-staging read-back
 * mirror [[GetCore]] exactly (one `SzEnvironment` per executor JVM, released on task completion via
 * `TaskContext`; the `getRecord` reads execute exactly once). A `Systemic` engine error fails the
 * task loudly (a Spark task retry handles a transient outage; the reads are idempotent); ANY other
 * non-systemic error is treated as "still exists" — never delete on doubt.
 *
 * Injected into the sink as its `verifyDeparted` seam so [[AbstractDeltaSink]] itself stays
 * engine-free (a pure-Delta sink keeps the Phase-1 identity default). Input/output frames carry the
 * `(data_source, record_id)` columns the reconcile delete keys on.
 */
object DepartedVerify {

  def run(spark: SparkSession, candidates: DataFrame, stagingPath: String): DataFrame = {
    import spark.implicits._
    val keys: Dataset[(String, String)] =
      candidates.select(col("data_source").as[String], col("record_id").as[String]).distinct()

    val gone: Dataset[(String, String)] = keys.mapPartitions { it =>
      val env = SzEngineProvider.acquire()
      Option(TaskContext.get())
        .foreach(_.addTaskCompletionListener[Unit](_ => SzEngineProvider.release()))
      val engine = env.getEngine()
      it.filter { case (ds, rid) => isGone(engine, ds, rid) }
    }

    // One committed action — the only place the engine reads execute — then read back (no re-exec).
    gone.write.mode(SaveMode.Overwrite).parquet(stagingPath)
    spark.read.parquet(stagingPath).toDF("data_source", "record_id")
  }

  private def isGone(engine: SzEngine, ds: String, rid: String): Boolean =
    try {
      SzEngineProvider.withReadLock(engine.getRecord(SzRecordKey.of(ds, rid), SzFlag.SZ_NO_FLAGS))
      false // record still exists (moved) — do NOT delete; the gaining entity's refresh re-keys it
    } catch {
      case _: SzNotFoundException => true // genuinely gone — safe to delete
      case t: Throwable =>
        ErrorTaxonomy.classify(t) match {
          case ErrorCategory.Systemic => throw t // real outage — fail the task loudly (idempotent)
          case _ => false // uncertain — never delete on doubt
        }
    }
}
