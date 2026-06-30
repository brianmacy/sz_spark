package com.senzing.spark.jobs

import org.apache.spark.TaskContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import com.senzing.spark.model.{StagingKind, StagingRow}
import com.senzing.spark.work.{InputRecord, RecordWorker}

/** The good (output) and error DataFrames produced from one engine pass. */
final case class SplitResult(good: DataFrame, errors: DataFrame)

/**
 * The single shared engine-execution pipeline (option B). The Senzing verb is a side effect that
 * must run EXACTLY ONCE per record, so we call it once in `mapPartitions`, emit one tagged-union
 * [[StagingRow]] stream, write it in a SINGLE committed action, then read that committed table back
 * and split into output/error — zero lineage re-execution, one commit per attempt.
 *
 * Engine lifetime is bracketed per partition: `acquire` at partition start, `release` on task
 * completion (via `TaskContext`, so it fires after the lazy iterator is fully consumed and on
 * failure) — never a premature `finally`.
 */
object SparkRecordOps {

  def run(
      spark: SparkSession,
      input: Dataset[InputRecord],
      stagingPath: String,
      mkWorker: () => RecordWorker,
      acquire: () => Unit = () => (),
      release: () => Unit = () => ()
  ): SplitResult = {
    import spark.implicits._

    val staged: Dataset[StagingRow] = input.mapPartitions { it =>
      acquire()
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => release()))
      val worker = mkWorker()
      it.flatMap(worker.processOne)
    }

    // One committed action — the only place the engine verb executes.
    staged.write.mode(SaveMode.Overwrite).parquet(stagingPath)

    // Read the committed table back and split (no engine re-execution).
    val back = spark.read.parquet(stagingPath)
    SplitResult(
      good = back.filter(col("kind") =!= StagingKind.Error),
      errors = back.filter(col("kind") === StagingKind.Error)
    )
  }
}
