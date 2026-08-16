package com.senzing.spark.core

import org.apache.spark.TaskContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import com.senzing.spark.model.{StagingKind, StagingRow}
import com.senzing.spark.work.{InputRecord, RecordWorker}

/**
 * The good (output) and error DataFrames produced from one engine pass, plus an [[unpersist]] hook
 * that frees the materialized cache backing them. The caller MUST invoke `unpersist()` once it has
 * consumed `good`/`errors` (written the sinks), or the cached blocks leak across chunks.
 */
final case class SplitResult(good: DataFrame, errors: DataFrame, unpersist: () => Unit = () => ())

/**
 * The single shared engine-execution pipeline. The Senzing verb is a side effect that must run
 * EXACTLY ONCE per record, so we call it once in `mapPartitions`, emit one tagged-union
 * [[com.senzing.spark.model.StagingRow]] stream, MATERIALIZE it once, then split into output/error
 * from the materialized result — zero lineage re-execution, one engine pass per attempt.
 *
 * MATERIALIZATION = Spark's distributed cache (`persist(MEMORY_AND_DISK)` + a `count()` to force
 * the single pass), NOT a host-local staging file. This is deliberate: on a multi-host Spark
 * cluster the driver + executors span both hosts, so a host-local parquet staging file written by
 * an executor on one host is invisible to an executor on the other (`FileNotFoundException`), and a
 * shared NFS path fails because the container uid is squashed (`Mkdirs failed`). The block manager
 * materializes the result in memory (spilling to Spark-managed local disk, refetched cross-host via
 * the block-manager RPC) with no filesystem path at all, so it works regardless of which executors
 * land the write and the read.
 *
 * Engine lifetime is bracketed per partition: `acquire` at partition start, `release` on task
 * completion (via `TaskContext`, so it fires after the lazy iterator is fully consumed and on
 * failure) — never a premature `finally`. Because the engine pass runs exactly once (at `count()`)
 * and the two filters read the cache, the bracket also runs exactly once per partition.
 */
object SparkRecordOps {

  def run(
      spark: SparkSession,
      input: Dataset[InputRecord],
      mkWorker: () => RecordWorker,
      acquire: () => Unit = () => (),
      release: () => Unit = () => ()
  ): SplitResult = {
    import spark.implicits._

    val staged: Dataset[StagingRow] = input
      .mapPartitions { it =>
        acquire()
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => release()))
        val worker = mkWorker()
        it.flatMap(worker.processOne)
      }
      // Materialize in Spark's distributed store (NOT a host-local file — see class doc).
      .persist(StorageLevel.MEMORY_AND_DISK)

    // Force the SINGLE engine pass now, so the good/errors filters read the cache and never
    // re-execute the mapPartitions (which would double-run the Senzing verb).
    staged.count()

    SplitResult(
      good = staged.filter(col("kind") =!= StagingKind.Error).toDF(),
      errors = staged.filter(col("kind") === StagingKind.Error).toDF(),
      unpersist = () => { staged.unpersist(blocking = false); () }
    )
  }
}
