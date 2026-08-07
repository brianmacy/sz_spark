package com.senzing.spark.glue

import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, SparkSession}

import com.senzing.spark.core.{AddCore, SplitResult}
import com.senzing.spark.jobs.SparkJob
import com.senzing.spark.work.InputRecord

/** Outcome counts from a feeder run — returned for tests and logged at shutdown. */
final case class FeederStats(processed: Long, failed: Long, reclaimed: Int)

/**
 * Glue Stage 2 (parallel): an OVERLAPPING-BATCH feeder over the parquet inbox → `core.AddCore`, the
 * tail-killing alternative to [[ParquetStreamFeeder]]. Structured Streaming commits micro-batches
 * strictly sequentially, so a batch finishes only when its slowest (huge-entity) shard does — the
 * fast slots idle at the tail (measured 76% host idle on `.142`). This feeder instead runs K driver
 * threads that each own ONE shard end-to-end (claim → read → add → sinks → dispose); a slow shard
 * holds a single thread while the other K-1 keep claiming, so freed slots refill immediately and
 * the only idle is genuine end-of-input. Each shard is one parquet file ⇒ one Spark partition ⇒ one
 * task ⇒ one core, so K concurrent jobs fill K cores — set `concurrency` = the engine slot count.
 *
 * OWNERSHIP WITHOUT A LOCK (per-unit dispose flavor; the parquet/RabbitMQ-via-drainer source):
 *   - claim: atomic `rename(inbox/part-X → processing/part-X)` — the rename is the mutual
 *     exclusion.
 *   - process: read the one shard file, `AddCore.run` into a PER-UNIT staging dir `staging/<unit>`
 *     (a shared staging path would clobber under concurrency — [[SparkRecordOps]] writes it
 *     `Overwrite`), split into good/error frames.
 *   - sinks: write each unit's error/good frames as their OWN atomically-renamed single files
 *     (`de-*.parquet` / `af-*.parquet`) via [[ShardIo.writeSingleFile]] — NEVER `Append`, which
 *     would race the commit protocol across concurrent jobs. Empty frame ⇒ no file. Schema is
 *     identical to what `ParquetStreamFeeder` wrote, so [[DeadLetterReprocess]] and downstream
 *     dedup are unchanged.
 *   - dispose: archive (if `archive` set) or delete the shard, then drop its staging dir.
 *   - reclaim: on start, move any shard left in `processing/` (a prior run's in-flight work) back
 *     to the inbox before claiming. A shard whose processing THROWS is left in `processing/` and
 *     reclaimed on the next restart — at-least-once (re-adding a resolved record is a fast
 *     optimized no-op), never dropped.
 *
 * `trigger` mirrors `ParquetStreamFeeder`: `default` = long-running (poll forever, the `.142`
 * mode); `availableNow` = drain the inbox then exit after `emptyMs` idle (scheduled batch + tests).
 *
 * Args: `inbox`, `processing`, `archive` (opt), `staging`, `deadLetter` (opt), `output` (opt),
 * `concurrency` (K, default 32), `trigger` (default long-running), `emptyMs` (30000), `runId`.
 */
object ParquetParallelFeeder extends SparkJob {

  private val IdlePauseMs = 200L
  private val ProgressEvery = 500L

  // scalastyle:off println
  private def log(msg: String): Unit = println(s"[ParquetParallelFeeder] $msg")
  private def logErr(msg: String): Unit =
    Console.err.println(s"[ParquetParallelFeeder] ERROR: $msg")
  // scalastyle:on println

  /**
   * Run the feeder. `process` (the engine pass) is injected — `(records, perUnitStagingPath) =>
   * SplitResult` — so the claim/dispose/reclaim/sink plumbing is testable without a real engine.
   */
  def run(
      spark: SparkSession,
      inbox: String,
      processing: String,
      archive: String,
      staging: String,
      deadLetter: String,
      output: String,
      concurrency: Int,
      trigger: String,
      emptyMs: Long,
      process: (Dataset[InputRecord], String) => SplitResult
  ): FeederStats = {
    import spark.implicits._
    require(concurrency > 0, s"concurrency must be > 0, was $concurrency")

    val fs = ShardIo.fileSystem(spark, inbox)
    val inboxPath = new Path(inbox)
    val processingPath = new Path(processing)
    val archiveOpt = if (archive.nonEmpty) Some(new Path(archive)) else None
    val schema = spark.emptyDataset[InputRecord].schema
    val availableNow = trigger == "availableNow"

    if (!fs.exists(processingPath)) fs.mkdirs(processingPath)
    val reclaimed = ShardIo.reclaim(fs, processingPath, inboxPath)
    if (reclaimed > 0) log(s"reclaimed $reclaimed orphaned shard(s) from a prior run -> inbox")

    val claimer = new ShardIo.Claimer(fs, inboxPath, processingPath)
    val processed = new AtomicLong(0)
    val failed = new AtomicLong(0)
    val running = new AtomicBoolean(true)
    val lastClaimMs = new AtomicLong(System.currentTimeMillis())

    def processOne(claimed: Path): Unit = {
      val unit = claimed.getName.stripSuffix(".parquet") // "part-<uuid>"
      val unitStaging = new Path(staging, unit).toString
      try {
        val ds = spark.read.schema(schema).parquet(claimed.toString).as[InputRecord]
        val result = process(ds, unitStaging)
        // Per-unit single-file sinks (unique names) — concurrency-safe, unlike Append.
        if (deadLetter.nonEmpty) ShardIo.writeSingleFile(spark, result.errors, deadLetter, "de")
        if (output.nonEmpty) ShardIo.writeSingleFile(spark, result.good, output, "af")
        ShardIo.dispose(fs, claimed, archiveOpt) // shard is now in the engine — archive/delete it
        ShardIo.deleteQuietly(fs, new Path(unitStaging)) // drop the transient per-unit staging
        val n = processed.incrementAndGet()
        if (n % ProgressEvery == 0) log(s"processed $n shard(s), ${failed.get()} failed")
      } catch {
        case NonFatal(e) =>
          // Leave it in processing/ → reclaimed + reprocessed on restart. NEVER dropped.
          failed.incrementAndGet()
          logErr(s"shard ${claimed.getName} left in processing/ for restart-reclaim: $e")
      }
    }

    def workerLoop(idx: Int): Unit = {
      // Each worker's jobs go to their own FAIR pool so K concurrent jobs share the cluster fairly
      // (belt-and-suspenders: each shard is a single-task job, so with cores >= K all K run at once
      // regardless — this only matters if a shard ever splits into >1 partition).
      spark.sparkContext.setLocalProperty("spark.scheduler.pool", s"feeder-$idx")
      while (running.get())
        claimer.claimNext() match {
          case Some(claimed) =>
            lastClaimMs.set(System.currentTimeMillis())
            processOne(claimed)
          case None =>
            if (availableNow && System.currentTimeMillis() - lastClaimMs.get() >= emptyMs)
              running.set(false) // drained: stop all workers
            else Thread.sleep(IdlePauseMs)
        }
    }

    log(s"starting: concurrency=$concurrency trigger=$trigger inbox=$inbox")
    val pool = Executors.newFixedThreadPool(concurrency)
    try {
      val workers =
        (0 until concurrency).map(i =>
          pool.submit(new Runnable { def run(): Unit = workerLoop(i) })
        )
      workers.foreach(_.get()) // blocks forever for trigger=default; until drained for availableNow
    } finally {
      pool.shutdown()
      pool.awaitTermination(1, TimeUnit.MINUTES)
    }
    val stats = FeederStats(processed.get(), failed.get(), reclaimed)
    log(
      s"exiting: processed=${stats.processed} failed=${stats.failed} reclaimed=${stats.reclaimed}"
    )
    stats
  }

  def main(args: Array[String]): Unit = {
    val m = GlueArgs.parse(args)
    val runId = m.getOrElse("runId", "run")
    val spark = buildSession(
      "sz-parquet-parallel-feeder",
      extraConf = Map("spark.scheduler.mode" -> "FAIR")
    )
    try
      run(
        spark,
        inbox = m.getOrElse("inbox", ""),
        processing = m.getOrElse("processing", ""),
        archive = m.getOrElse("archive", ""),
        staging = m.getOrElse("staging", "staging"),
        deadLetter = m.getOrElse("deadLetter", ""),
        output = m.getOrElse("output", ""),
        concurrency = m.getOrElse("concurrency", "32").toInt,
        trigger = m.getOrElse("trigger", "default"),
        emptyMs = m.getOrElse("emptyMs", "30000").toLong,
        process = (ds, stagingPath) => AddCore.run(spark, ds, runId, stagingPath)
      )
    finally spark.stop()
  }
}
