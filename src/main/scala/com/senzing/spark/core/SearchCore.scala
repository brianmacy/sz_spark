package com.senzing.spark.core

import org.apache.spark.sql.{Dataset, SparkSession}

import com.senzing.spark.engine.SzEngineProvider
import com.senzing.spark.work.{InputRecord, WorkerOp}

/**
 * Core search: `searchByAttributes` per request over a transport-agnostic `Dataset[InputRecord]`
 * (payload = attributes JSON) → (request, result) good frame + error frame.
 */
object SearchCore {
  def run(
      spark: SparkSession,
      input: Dataset[InputRecord],
      runId: String,
      stagingPath: String
  ): SplitResult =
    SparkRecordOps.run(
      spark,
      input,
      stagingPath,
      EngineWorker.factory(WorkerOp.Search, runId, Verbs.search),
      acquire = () => (),
      release = () => SzEngineProvider.release()
    )
}
