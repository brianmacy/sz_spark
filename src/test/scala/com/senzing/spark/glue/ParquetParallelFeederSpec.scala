package com.senzing.spark.glue

import java.io.File
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import com.senzing.spark.core.SplitResult
import com.senzing.spark.model.{AffectedEntityRow, ErrorRow}
import com.senzing.spark.work.{ErrorCategory, InputRecord}

/**
 * Spark closures must capture only a serializable top-level object — never the ScalaTest instance —
 * so the injected fake engine passes and their shared latches/counters live here. The real engine
 * (`AddCore`) stays out of `sbt test`: the claim/dispose/reclaim/sink plumbing AND the overlap
 * (no-head-of-line-block) guarantee are exercised with fakes that do no native init.
 */
object ParquetParallelFeederSpec {
  val rowCount = new AtomicLong(0)
  val overlapLatch = new CountDownLatch(2) // the two fast shards in the overlap test
  @volatile var slowSawLatchZero = false

  /**
   * Counts rows; commits to the PER-UNIT staging like AddCore (so a shared-staging clobber under
   * concurrency would corrupt counts), then returns an empty split.
   */
  def countingProcess(ds: Dataset[InputRecord], staging: String): SplitResult = {
    val ss = ds.sparkSession
    ds.write.mode(SaveMode.Overwrite).parquet(staging) // exercise the per-unit staging path
    rowCount.addAndGet(ss.read.parquet(staging).count())
    val empty = ss.emptyDataFrame
    SplitResult(empty, empty)
  }

  /**
   * `bad*` → BAD_INPUT error row, everything else → an affected-entity good row (via per-unit
   * staging, mirroring AddCore's commit-then-read-back split).
   */
  def splitProcess(ds: Dataset[InputRecord], staging: String): SplitResult = {
    val ss = ds.sparkSession
    import ss.implicits._
    ds.write.mode(SaveMode.Overwrite).parquet(staging)
    val back = ss.read.parquet(staging).as[InputRecord]
    val errors = back
      .filter(_.recordId.startsWith("bad"))
      .map(r =>
        ErrorRow(r.dataSource, r.recordId, r.payload, ErrorCategory.BadInput.name, "", "e", 0)
      )
      .toDF()
    val good = back
      .filter(!_.recordId.startsWith("bad"))
      .map(r => AffectedEntityRow(r.dataSource, r.recordId, 1L, "ADD", "run"))
      .toDF()
    SplitResult(good, errors)
  }

  /**
   * Throws on any shard containing a `boom` record (a systemic-style failure) — the shard must be
   * left in `processing/`, never dropped, and must not stop the other shards.
   */
  def failProcess(ds: Dataset[InputRecord], staging: String): SplitResult = {
    val ids = ds.collect().map(_.recordId).toSet
    if (ids.contains("boom")) throw new RuntimeException("boom")
    val ss = ds.sparkSession
    import ss.implicits._
    val good =
      ss.createDataset(ids.toSeq).map(id => AffectedEntityRow("T", id, 1L, "ADD", "run")).toDF()
    SplitResult(good, ss.emptyDataFrame)
  }

  /**
   * The slow shard blocks until BOTH fast shards have run — proving they were not queued behind it
   * (head-of-line blocking). With concurrency=2 and one slow shard a free worker always exists.
   */
  def overlapProcess(ds: Dataset[InputRecord], staging: String): SplitResult = {
    val ss = ds.sparkSession
    import ss.implicits._
    val ids = ds.collect().map(_.recordId).toSet
    if (ids.contains("slow")) slowSawLatchZero = overlapLatch.await(15, TimeUnit.SECONDS)
    else overlapLatch.countDown()
    val good =
      ss.createDataset(ids.toSeq).map(id => AffectedEntityRow("T", id, 1L, "ADD", "run")).toDF()
    SplitResult(good, ss.emptyDataFrame)
  }
}

final class ParquetParallelFeederSpec extends AnyFunSuite with BeforeAndAfterAll {
  import ParquetParallelFeederSpec._

  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession
      .builder()
      .appName("sz-parquet-parallel-feeder-test")
      .master("local[4]")
      .config("spark.ui.enabled", "false")
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

  override def afterAll(): Unit = if (spark != null) spark.stop()

  /**
   * Write one shard = one flat `part-<uuid>.parquet` FILE (the shape MqToParquet/ShardIo produce).
   */
  private def writeShard(dir: File, recs: Seq[InputRecord]): Unit = {
    val ss = spark
    import ss.implicits._
    dir.mkdirs()
    ShardIo.writeSingleFile(ss, ss.createDataset(recs).toDF(), dir.getAbsolutePath, "part")
  }

  private def partFiles(dir: File): Array[File] =
    Option(dir.listFiles()).getOrElse(Array.empty).filter { f =>
      f.getName.startsWith("part-") && f.getName.endsWith(".parquet")
    }

  private case class Dirs(inbox: File, processing: File, archive: File, staging: File)
  private def dirs(): Dirs = {
    val base = Files.createTempDirectory("pfeed").toFile
    Dirs(
      new File(base, "inbox"),
      new File(base, "processing"),
      new File(base, "archive"),
      new File(base, "staging")
    )
  }

  private def feed(
      d: Dirs,
      process: (Dataset[InputRecord], String) => SplitResult,
      concurrency: Int = 2,
      deadLetter: String = "",
      output: String = "",
      archive: String = ""
  ): FeederStats =
    ParquetParallelFeeder.run(
      spark,
      inbox = d.inbox.getAbsolutePath,
      processing = d.processing.getAbsolutePath,
      archive = archive,
      staging = d.staging.getAbsolutePath,
      deadLetter = deadLetter,
      output = output,
      concurrency = concurrency,
      trigger = "availableNow",
      emptyMs = 500L,
      process = process
    )

  test("claims + processes every shard once, then drains inbox and clears processing/staging") {
    val d = dirs()
    writeShard(d.inbox, Seq(InputRecord("T", "a", "{}"), InputRecord("T", "b", "{}")))
    writeShard(d.inbox, Seq(InputRecord("T", "c", "{}")))
    writeShard(d.inbox, Seq(InputRecord("T", "d", "{}")))

    rowCount.set(0)
    val stats = feed(d, countingProcess, concurrency = 3)

    assert(stats.processed == 3L, s"all 3 shards processed, was ${stats.processed}")
    assert(rowCount.get() == 4L, s"all 4 rows fed exactly once, was ${rowCount.get()}")
    assert(partFiles(d.inbox).isEmpty, "inbox drained")
    assert(partFiles(d.processing).isEmpty, "no shard left in processing/ (all disposed)")
    // per-unit staging dirs cleaned up
    assert(
      !d.staging.exists() || Option(d.staging.listFiles()).forall(_.isEmpty),
      "per-unit staging dirs were cleaned"
    )
  }

  test("archive mode moves disposed shards into archive/ (not deleted)") {
    val d = dirs()
    writeShard(d.inbox, Seq(InputRecord("T", "a", "{}")))
    writeShard(d.inbox, Seq(InputRecord("T", "b", "{}")))

    val stats = feed(d, splitProcess, archive = d.archive.getAbsolutePath)
    assert(stats.processed == 2L)
    assert(partFiles(d.inbox).isEmpty && partFiles(d.processing).isEmpty)
    assert(partFiles(d.archive).length == 2, "both disposed shards archived")
  }

  test("dead-letter + output sinks: per-shard files accumulate, carry category + recordId") {
    val d = dirs()
    val deadLetter = new File(d.inbox.getParentFile, "deadletter")
    val output = new File(d.inbox.getParentFile, "affected")
    writeShard(d.inbox, Seq(InputRecord("T", "bad-1", "{}"), InputRecord("T", "good-1", "{}")))
    writeShard(d.inbox, Seq(InputRecord("T", "bad-2", "{}"), InputRecord("T", "good-2", "{}")))

    val stats = feed(
      d,
      splitProcess,
      deadLetter = deadLetter.getAbsolutePath,
      output = output.getAbsolutePath
    )
    assert(stats.processed == 2L)

    val dl = spark.read.parquet(deadLetter.getAbsolutePath)
    val out = spark.read.parquet(output.getAbsolutePath)
    assert(dl.count() == 2L, s"both shards' error rows accumulated, was ${dl.count()}")
    assert(out.count() == 2L, s"both shards' good rows accumulated, was ${out.count()}")
    val cats = dl.collect().map(_.getAs[String]("category")).toSet
    assert(cats == Set(ErrorCategory.BadInput.name), s"error rows carry their category, was $cats")
    val ids = dl.collect().map(_.getAs[String]("recordId")).toSet
    assert(ids == Set("bad-1", "bad-2"), s"exactly the failed records dead-lettered, was $ids")
  }

  test("reclaims a prior run's in-flight shard from processing/ before claiming") {
    val d = dirs()
    // Simulate a crash: a shard was claimed (in processing/) but never disposed.
    writeShard(
      d.processing,
      Seq(InputRecord("T", "orphan-1", "{}"), InputRecord("T", "orphan-2", "{}"))
    )
    writeShard(d.inbox, Seq(InputRecord("T", "fresh", "{}")))

    rowCount.set(0)
    val stats = feed(d, countingProcess)
    assert(stats.reclaimed == 1, s"the orphaned shard was reclaimed, was ${stats.reclaimed}")
    assert(stats.processed == 2L, s"reclaimed + fresh shard both processed, was ${stats.processed}")
    assert(rowCount.get() == 3L, s"all 3 rows (2 orphan + 1 fresh) fed, was ${rowCount.get()}")
    assert(partFiles(d.processing).isEmpty, "processing/ empty after disposal")
  }

  test("a failing shard is left in processing/ (never dropped) and does not stop the others") {
    val d = dirs()
    writeShard(d.inbox, Seq(InputRecord("T", "boom", "{}"))) // fails
    writeShard(d.inbox, Seq(InputRecord("T", "ok-1", "{}")))
    writeShard(d.inbox, Seq(InputRecord("T", "ok-2", "{}")))

    val stats = feed(d, failProcess, concurrency = 2)
    assert(stats.failed == 1L, s"the boom shard failed, was ${stats.failed}")
    assert(stats.processed == 2L, s"the other two shards still completed, was ${stats.processed}")
    assert(partFiles(d.inbox).isEmpty, "inbox drained (boom shard was claimed)")
    assert(
      partFiles(d.processing).length == 1,
      "the failed shard is retained in processing/ for restart-reclaim"
    )
  }

  test("overlap: a slow shard does not head-of-line-block the fast ones (concurrency=2)") {
    val d = dirs()
    writeShard(d.inbox, Seq(InputRecord("T", "slow", "{}")))
    writeShard(d.inbox, Seq(InputRecord("T", "fast-1", "{}")))
    writeShard(d.inbox, Seq(InputRecord("T", "fast-2", "{}")))

    slowSawLatchZero = false
    val stats = feed(d, overlapProcess, concurrency = 2)
    assert(stats.processed == 3L, s"all three shards processed, was ${stats.processed}")
    assert(
      slowSawLatchZero,
      "the two fast shards ran WHILE the slow shard blocked (no head-of-line blocking / no tail)"
    )
  }
}
