package com.senzing.spark.core

import org.apache.spark.sql.{Dataset, SparkSession}

import com.senzing.spark.engine.SzEngineProvider
import com.senzing.spark.work.{InputRecord, WorkerOp}

/**
 * Core delete: `deleteRecord` per record (WITH_INFO) over a transport-agnostic
 * `Dataset[InputRecord]` → deduped affected-entity good frame + error frame.
 */
object DeleteCore {
  def run(
      spark: SparkSession,
      input: Dataset[InputRecord],
      runId: String
  ): SplitResult =
    SparkRecordOps.run(
      spark,
      input,
      EngineWorker.factory(WorkerOp.Delete, runId, Verbs.delete),
      acquire = () => (),
      release = () => SzEngineProvider.release()
    )
}
