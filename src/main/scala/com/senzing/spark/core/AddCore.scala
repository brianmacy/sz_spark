package com.senzing.spark.core

import org.apache.spark.sql.{Dataset, SparkSession}

import com.senzing.spark.engine.SzEngineProvider
import com.senzing.spark.work.{InputRecord, WorkerOp}

/**
 * Core add/update: `addRecord` per record (WITH_INFO) over a transport-agnostic
 * `Dataset[InputRecord]` → deduped affected-entity good frame + error frame. No transport, no file
 * formats — glue supplies the DataFrame.
 */
object AddCore {
  def run(
      spark: SparkSession,
      input: Dataset[InputRecord],
      runId: String
  ): SplitResult =
    SparkRecordOps.run(
      spark,
      input,
      EngineWorker.factory(WorkerOp.Add, runId, Verbs.add),
      acquire = () => (),
      release = () => SzEngineProvider.release()
    )
}
