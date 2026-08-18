package com.senzing.spark.jobs

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}

import com.senzing.spark.core.{AddCore, SplitResult}
import com.senzing.spark.glue.{DeadLetterReprocess, GlueArgs, ParquetStreamFeeder, ShardIo}
import com.senzing.spark.work.InputRecord

/**
 * Single-process dead-letter reprocessor.
 *
 * Reprocessing the dead-letter queue tends to CREATE MORE dead letters — a re-driven record can
 * fail again — so a one-pass sweep is not enough and a naive loop would re-drive genuinely-stuck
 * records forever (`DeadLetterReprocess.selectReprocessable` drops `attempts`, so a re-failed
 * record gets a fresh `ErrorRow` with no cross-generation memory). This job wraps
 * [[DeadLetterReprocess]] in a BOUNDED, CONVERGENT loop:
 *
 * round r: sweep generation r's DLQ into a re-feed inbox (reprocessable categories only:
 * RETRY_EXHAUSTED / CONFIG_RELEVANT / REPLACE_CONFLICT — BAD_INPUT / NOT_FOUND stay terminal) →
 * re-drive that inbox through the engine SERIALLY → route this round's failures into generation r+1
 * → repeat.
 *
 * Why single-process is the point, not just a nicety: the re-drive runs `master=local[1]` and the
 * input is coalesced to one partition, so the Senzing verb runs on ONE thread with the engine
 * initialized once. That REMOVES the concurrency that produces `REPLACE_CONFLICT` (the dominant
 * reprocessable category is a lock/replace race), and gives `RETRY_EXHAUSTED` a fresh budget. So a
 * serial pass drains the bulk of the queue by construction.
 *
 * Termination (bounded rounds + shrink check — no schema change):
 *   - DRAINED: the sweep finds nothing reprocessable, or a round's residue is empty.
 *   - NOT SHRINKING: a round's reprocessable residue did not fall below what it re-drove (zero
 *     progress) → stop; the residue is genuinely stuck.
 *   - CAP: `maxRounds` reached.
 * On any non-drained stop the final generation is moved to `quarantine` for human review and is
 * never re-driven again. Swept originals accumulate in per-round archive dirs as the audit trail.
 *
 * Idempotent: [[DeadLetterReprocess.run]] archives each swept snapshot, and `addRecord` is
 * idempotent on (DATA_SOURCE, RECORD_ID), so a re-run resumes safely.
 *
 * Args (key=value): `deadLetter=` (input DLQ dir) `work=` (scratch root for per-round refeed/gen/
 * archive dirs) `quarantine=` (terminal residue dir) `output=` (optional `$AFFECTED` feed to append
 * re-driven successes to, so the entity-mart sees them) `runId=` `maxRounds=` (default 3).
 */
object DeadLetterJob extends SparkJob {

  /** Result of a reprocess run, for logging/tests. */
  final case class Outcome(
      rounds: Int,
      reFedTotal: Long,
      quarantined: Long,
      drained: Boolean
  )

  /** Count the reprocessable rows currently sitting in a DLQ-shaped dir (0 if absent/empty). */
  private def reprocessableCount(spark: SparkSession, dir: String): Long = {
    val fs = ShardIo.fileSystem(spark, dir)
    val shards = ShardIo.listShards(fs, new Path(dir))
    if (shards.isEmpty) 0L
    else
      spark.read
        .parquet(shards.map(_.toString): _*)
        .filter(col("category").isInCollection(DeadLetterReprocess.Reprocessable))
        .count()
  }

  /** Move every shard under `dir` into `quarantine` (rename); returns the count moved. */
  private def quarantineDir(spark: SparkSession, dir: String, quarantine: String): Long = {
    val fs = ShardIo.fileSystem(spark, dir)
    val shards = ShardIo.listShards(fs, new Path(dir))
    val qPath = new Path(quarantine)
    shards.foreach(shard => ShardIo.dispose(fs, shard, Some(qPath)))
    shards.length.toLong
  }

  /**
   * The convergent reprocess loop. `reDrive` is injected (default [[AddCore.run]]) so the loop is
   * testable without a real engine. The input handed to `reDrive` is already coalesced to a single
   * partition for serial, single-process execution.
   */
  def run(
      spark: SparkSession,
      deadLetter: String,
      work: String,
      quarantine: String,
      output: String,
      runId: String,
      maxRounds: Int,
      reDrive: Dataset[InputRecord] => SplitResult
  ): Outcome = {
    import spark.implicits._
    require(maxRounds > 0, s"maxRounds must be > 0, was $maxRounds")

    var gen = deadLetter
    var round = 0
    var reFedTotal = 0L
    var quarantined = 0L
    var drained = false
    var stop = false

    while (!stop) {
      val reFeed = s"$work/refeed-$round"
      val archive = s"$work/archived-$round"
      val nextGen = s"$work/gen-${round + 1}"

      // 1) Sweep generation `gen` -> re-feed inbox (reprocessable only; originals archived).
      val swept = DeadLetterReprocess.run(spark, gen, reFeed, archive)
      if (swept == 0L) {
        // Nothing reprocessable left in this generation -> fully drained.
        drained = true
        stop = true
      } else {
        reFedTotal += swept

        // 2) Re-drive the inbox SERIALLY (single partition), then route outputs:
        //    failures -> next generation DLQ (with failedAt/source stamps), successes -> $AFFECTED.
        val input = spark.read.parquet(reFeed).as[InputRecord].coalesce(1)
        val res = reDrive(input)
        try ParquetStreamFeeder.writeSinks(res, deadLetter = nextGen, output = output)
        finally res.unpersist()

        // 3) Convergence check on the new generation's reprocessable residue.
        val residue = reprocessableCount(spark, nextGen)
        if (residue == 0L) {
          drained = true
          stop = true
        } else if (residue >= swept) {
          // Zero progress this round -> genuinely stuck. Quarantine and stop.
          quarantined += quarantineDir(spark, nextGen, quarantine)
          stop = true
        } else if (round + 1 >= maxRounds) {
          quarantined += quarantineDir(spark, nextGen, quarantine)
          stop = true
        } else {
          gen = nextGen
          round += 1
        }
      }
    }

    Outcome(
      rounds = round + 1,
      reFedTotal = reFedTotal,
      quarantined = quarantined,
      drained = drained
    )
  }

  def main(args: Array[String]): Unit = {
    val m = GlueArgs.parse(args)
    val deadLetter = m.getOrElse("deadLetter", "")
    val work = m.getOrElse("work", s"$deadLetter-reprocess")
    val quarantine = m.getOrElse("quarantine", s"$deadLetter-quarantine")
    val output = m.getOrElse("output", "")
    val runId = m.getOrElse("runId", "dlq-reprocess")
    val maxRounds = m.getOrElse("maxRounds", "3").toInt

    // local[1] = one executor thread = the engine verb runs on a single process, serially. This is
    // the whole point: it cures the REPLACE_CONFLICT concurrency and gives RETRY_EXHAUSTED a fresh
    // budget, so the queue drains instead of thrashing.
    val spark = buildSession("sz-dead-letter-reprocess", master = Some("local[1]"))
    try {
      val o = run(
        spark,
        deadLetter,
        work,
        quarantine,
        output,
        runId,
        maxRounds,
        reDrive = (in: Dataset[InputRecord]) => AddCore.run(spark, in, runId)
      )
      spark.sparkContext.setJobDescription(
        s"dead-letter reprocess: ${o.rounds} rounds, re-fed ${o.reFedTotal}, quarantined ${o.quarantined}"
      )
      // scalastyle:off println
      println(
        s"DEAD_LETTER_JOB rounds=${o.rounds} reFed=${o.reFedTotal} quarantined=${o.quarantined} " +
          s"drained=${o.drained} (work=$work quarantine=$quarantine)"
      )
      // scalastyle:on println
    } finally spark.stop()
  }
}
