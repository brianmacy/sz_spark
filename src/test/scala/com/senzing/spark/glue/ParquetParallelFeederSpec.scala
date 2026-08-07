package com.senzing.spark.glue

import java.io.File
import java.nio.file.Files
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import com.senzing.spark.core.SplitResult
import com.senzing.spark.model.{AffectedEntityRow, ErrorRow}
import com.senzing.spark.work.{ErrorCategory, InputRecord}

/**
 * Spark closures must capture only a serializable top-level object — never the ScalaTest instance —
 * so the fake source, fake engine passes, and their shared latches/counters live here. The real
 * engine (`AddCore`) stays out of `sbt test`: the source-agnostic [[OverlappingBatchEngine]]
 * plumbing (overlap, per-chunk staging, sinks, commit, no-drop) is exercised against a FAKE
 * in-memory source (no filesystem — proving source-agnosticism), and [[InboxSource]]'s
 * claim/dispose/reclaim is exercised on a real temp filesystem.
 */
object ParquetParallelFeederSpec {
  val rowCount = new AtomicLong(0)
  val overlapLatch = new CountDownLatch(6) // the six fast chunks in the no-stall test
  @volatile var slowSawLatchZero = false

  /**
   * In-memory [[RecordSource]] — proves the engine needs no filesystem. Chunks are popped in order;
   * committed bounds are recorded; reclaim is a no-op (watermark-style).
   */
  final class MemSource(spark: SparkSession, chunks: Seq[(String, Seq[InputRecord])])
      extends RecordSource {
    private val queue = new ConcurrentLinkedQueue[(String, Seq[InputRecord])]()
    chunks.foreach(queue.add)
    val committed: java.util.Set[String] =
      java.util.concurrent.ConcurrentHashMap.newKeySet[String]()
    def initialCursor: String = ""
    def nextChunk(cursor: String): Option[Chunk] =
      Option(queue.poll()).map { case (id, recs) =>
        import spark.implicits._
        Chunk(id, spark.createDataset(recs), "")
      }
    def commit(bounds: String): Unit = { committed.add(bounds); () }
    def reclaim(): Unit = ()
  }

  /**
   * Counts rows via the PER-CHUNK staging (a shared staging path would corrupt counts under
   * concurrency); returns an empty split.
   */
  def countingProcess(ds: Dataset[InputRecord], staging: String): SplitResult = {
    val ss = ds.sparkSession
    ds.write.mode(SaveMode.Overwrite).parquet(staging)
    rowCount.addAndGet(ss.read.parquet(staging).count())
    val empty = ss.emptyDataFrame
    SplitResult(empty, empty)
  }

  /** `bad*` → BAD_INPUT error row, else an affected-entity good row (via per-chunk staging). */
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

  /** Throws on any chunk containing a `boom` record (a systemic-style failure). */
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
   * The slow chunk blocks until BOTH fast chunks have run — proving they were not queued behind it.
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
      .appName("sz-parallel-batch-feeder-test")
      .master("local[4]")
      .config("spark.ui.enabled", "false")
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def tmp(prefix: String): File = Files.createTempDirectory(prefix).toFile

  private def rec(id: String): InputRecord = InputRecord("T", id, "{}")

  private def partFiles(dir: File, prefix: String): Array[File] =
    Option(dir.listFiles()).getOrElse(Array.empty).filter { f =>
      f.getName.startsWith(prefix) && f.getName.endsWith(".parquet")
    }

  private def writeShard(dir: File, recs: Seq[InputRecord]): Unit = {
    val ss = spark
    import ss.implicits._
    dir.mkdirs()
    ShardIo.writeSingleFile(ss, ss.createDataset(recs).toDF(), dir.getAbsolutePath, "part")
  }

  private def runEngine(
      source: RecordSource,
      process: (Dataset[InputRecord], String) => SplitResult,
      maxUnprocessedBatches: Int = 2,
      deadLetter: String = "",
      output: String = ""
  ): OverlappingBatchEngine.Stats =
    OverlappingBatchEngine.run(
      spark,
      source,
      process,
      stagingBase = tmp("staging").getAbsolutePath,
      deadLetter = deadLetter,
      output = output,
      recordsPerBatch = 1000, // => 1 partition/batch (the production operating point; no shuffle)
      maxUnprocessedBatches = maxUnprocessedBatches,
      trigger = "availableNow",
      emptyMs = 500L
    )

  // ── engine, source-agnostic (in-memory source, no filesystem) ──────────────────────────────────

  test("engine processes every chunk once and commits each (in-memory source, no filesystem)") {
    val src = new MemSource(
      spark,
      Seq("c1" -> Seq(rec("a"), rec("b")), "c2" -> Seq(rec("c")), "c3" -> Seq(rec("d")))
    )
    rowCount.set(0)
    val stats = runEngine(src, countingProcess, maxUnprocessedBatches = 3)
    assert(stats.processedChunks == 3L, s"all 3 chunks processed, was ${stats.processedChunks}")
    assert(rowCount.get() == 4L, s"all 4 rows fed once, was ${rowCount.get()}")
    assert(src.committed.size() == 3, s"every chunk committed, was ${src.committed.size()}")
  }

  test("engine per-chunk sinks: error + good frames accumulate as unique files across chunks") {
    val dl = tmp("dl"); val out = tmp("out")
    val src = new MemSource(
      spark,
      Seq("c1" -> Seq(rec("bad-1"), rec("good-1")), "c2" -> Seq(rec("bad-2"), rec("good-2")))
    )
    val stats =
      runEngine(src, splitProcess, deadLetter = dl.getAbsolutePath, output = out.getAbsolutePath)
    assert(stats.processedChunks == 2L)
    val dlf = spark.read.parquet(dl.getAbsolutePath)
    val outf = spark.read.parquet(out.getAbsolutePath)
    assert(dlf.count() == 2L, s"both chunks' error rows accumulated, was ${dlf.count()}")
    assert(outf.count() == 2L, s"both chunks' good rows accumulated, was ${outf.count()}")
    val ids = dlf.collect().map(_.getAs[String]("recordId")).toSet
    assert(ids == Set("bad-1", "bad-2"), s"exactly the failed records dead-lettered, was $ids")
  }

  test("engine never drops a failed chunk: it is NOT committed and the others still commit") {
    val src = new MemSource(
      spark,
      Seq("boom" -> Seq(rec("boom")), "ok1" -> Seq(rec("x")), "ok2" -> Seq(rec("y")))
    )
    val stats = runEngine(src, failProcess, maxUnprocessedBatches = 2)
    assert(stats.failedChunks == 1L, s"the boom chunk failed, was ${stats.failedChunks}")
    assert(
      stats.processedChunks == 2L,
      s"the other two chunks committed, was ${stats.processedChunks}"
    )
    assert(
      !src.committed.contains("boom"),
      "the failed chunk was NOT committed (source will reclaim it)"
    )
    assert(
      src.committed.contains("ok1") && src.committed.contains("ok2"),
      "the good chunks committed"
    )
  }

  test("no-stall: a straggler blocks only its own worker while the others keep cycling batches") {
    // K=3 workers, 1 straggler + 6 fast batches (> K). The straggler parks ONE worker; the other two
    // must churn all 6 fast batches (which release the straggler's latch). Proves a straggler costs
    // one worker/slot, not the pool — the property that failed at K=10 with multi-partition batches.
    slowSawLatchZero = false
    val chunks = ("slow" -> Seq(rec("slow"))) +: (1 to 6).map(i => s"f$i" -> Seq(rec(s"fast-$i")))
    val src = new MemSource(spark, chunks)
    val stats = runEngine(src, overlapProcess, maxUnprocessedBatches = 3)
    assert(stats.processedChunks == 7L, s"all 7 chunks committed, was ${stats.processedChunks}")
    assert(
      slowSawLatchZero,
      "the 6 fast batches committed on 2 workers WHILE the straggler parked the 3rd (no stall)"
    )
  }

  // ── InboxSource (dispose flavor) on a real temp filesystem ─────────────────────────────────────

  private def inboxSource(
      base: File,
      filesPerChunk: Int,
      archive: String = ""
  ): (InboxSource, File, File) = {
    val inbox = new File(base, "inbox"); inbox.mkdirs()
    val processing = new File(base, "processing")
    // recordsPerShard=1 ⇒ recordsPerBatch == filesPerChunk, so the source claims exactly
    // `filesPerChunk` shard files per chunk (keeps the test's file-count intent).
    (
      new InboxSource(
        spark,
        inbox.getAbsolutePath,
        processing.getAbsolutePath,
        archive,
        recordsPerBatch = filesPerChunk,
        recordsPerShard = 1
      ),
      inbox,
      processing
    )
  }

  test("InboxSource claims up to filesPerChunk shards per chunk, then None when drained") {
    val base = tmp("inbox-claim")
    val (src, inbox, processing) = inboxSource(base, filesPerChunk = 2)
    (1 to 5).foreach(i => writeShard(inbox, Seq(rec(s"r$i"))))
    val c1 = src.nextChunk(""); val c2 = src.nextChunk(""); val c3 = src.nextChunk("");
    val c4 = src.nextChunk("")
    assert(c1.exists(_.df.count() == 2), "first chunk = 2 shards")
    assert(c2.exists(_.df.count() == 2), "second chunk = 2 shards")
    assert(c3.exists(_.df.count() == 1), "third chunk = the last shard")
    assert(c4.isEmpty, "inbox drained → None")
    assert(partFiles(inbox, "part-").isEmpty, "inbox emptied by claiming")
    assert(processing.exists(), "claimed shards live under processing/")
  }

  test("InboxSource commit archives the chunk's shards and clears processing/") {
    val base = tmp("inbox-commit")
    val archive = new File(base, "archive")
    val (src, inbox, processing) =
      inboxSource(base, filesPerChunk = 3, archive = archive.getAbsolutePath)
    (1 to 3).foreach(i => writeShard(inbox, Seq(rec(s"r$i"))))
    val chunk = src.nextChunk("").get
    src.commit(chunk.bounds)
    assert(partFiles(archive, "part-").length == 3, "all 3 shards archived on commit")
    val leftover = Option(processing.listFiles()).getOrElse(Array.empty)
    assert(
      leftover.forall(f => !f.getName.startsWith(chunk.bounds)),
      "the committed batch dir is gone"
    )
  }

  test("InboxSource reclaim moves a prior run's in-flight shards back to the inbox") {
    val base = tmp("inbox-reclaim")
    val (src, inbox, processing) = inboxSource(base, filesPerChunk = 10)
    // Simulate a crash: two claimed batch dirs never committed.
    val b1 = new File(processing, "batch-1"); val b2 = new File(processing, "batch-2")
    writeShard(b1, Seq(rec("o1"))); writeShard(b1, Seq(rec("o2"))); writeShard(b2, Seq(rec("o3")))
    src.reclaim()
    assert(partFiles(inbox, "part-").length == 3, "all 3 orphaned shards moved back to inbox")
    assert(!b1.exists() && !b2.exists(), "the emptied batch dirs were removed")
    // and the reclaimed shards are now claimable
    assert(src.nextChunk("").exists(_.df.count() == 3), "reclaimed shards are reprocessable")
  }
}
