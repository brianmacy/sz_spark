package com.senzing.spark.glue

import java.io.File
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import com.senzing.spark.core.SplitResult
import com.senzing.spark.model.{AffectedEntityRow, ErrorRow}
import com.senzing.spark.work.{ErrorCategory, InputRecord}

/**
 * Spark closures must capture only a serializable top-level object — never the ScalaTest instance —
 * so the injected fake engine passes and their counters live here. This keeps the real engine
 * (`AddCore`) out of the default `sbt test`: the plumbing (streaming read + archive + checkpoint)
 * AND the dead-letter/output sinks are exercised with a fake `process` that splits records without
 * an engine.
 */
object ParquetStreamFeederSpec {
  val rowCount = new AtomicLong(0)

  /** Counts rows, returns an empty split — for the plumbing (feed-once/checkpoint) assertions. */
  def countingSink(ds: Dataset[InputRecord]): SplitResult = {
    rowCount.addAndGet(ds.count())
    val empty = ds.sparkSession.emptyDataFrame
    SplitResult(empty, empty)
  }

  /**
   * Fake engine pass with NO engine: `bad*` → terminal BAD_INPUT error, `retry*` → RETRY_EXHAUSTED
   * error, everything else → an affected-entity good row. Lets the sink writes be asserted
   * directly.
   */
  def splitProcess(ds: Dataset[InputRecord]): SplitResult = {
    val ss = ds.sparkSession
    import ss.implicits._
    val retryCat = ErrorCategory.RetryExhausted.name
    val badCat = ErrorCategory.BadInput.name
    val addOp = "ADD"
    val errors = ds
      .filter(r => r.recordId.startsWith("bad") || r.recordId.startsWith("retry"))
      .map { r =>
        val cat = if (r.recordId.startsWith("retry")) retryCat else badCat
        ErrorRow(r.dataSource, r.recordId, r.payload, cat, "", "err", 0)
      }
      .toDF()
    val good = ds
      .filter(r => !(r.recordId.startsWith("bad") || r.recordId.startsWith("retry")))
      .map(r => AffectedEntityRow(r.dataSource, r.recordId, 1L, addOp, "run"))
      .toDF()
    SplitResult(good, errors)
  }
}

final class ParquetStreamFeederSpec extends AnyFunSuite with BeforeAndAfterAll {
  import ParquetStreamFeederSpec._

  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession
      .builder()
      .appName("sz-parquet-stream-feeder-test")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

  override def afterAll(): Unit = if (spark != null) spark.stop()

  /** Write the records as flat parquet FILES directly under `inbox` (one file per partition). */
  private def writeShards(inbox: File, recs: Seq[InputRecord], files: Int): Unit = {
    val ss = spark
    import ss.implicits._
    ss.createDataset(recs)
      .repartition(files)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(inbox.getAbsolutePath)
  }

  private def countFilesRecursive(dir: File): Int =
    if (!dir.exists()) 0
    else
      Option(dir.listFiles()).getOrElse(Array.empty).foldLeft(0) { (acc, f) =>
        acc + (if (f.isDirectory) countFilesRecursive(f) else 1)
      }

  private def feed(
      inbox: File,
      checkpoint: File,
      archive: File,
      process: Dataset[InputRecord] => SplitResult,
      deadLetter: String = "",
      output: String = "",
      maxFilesPerTrigger: Int = 10
  ): Unit =
    ParquetStreamFeeder
      .run(
        spark,
        inbox = inbox.getAbsolutePath,
        checkpoint = checkpoint.getAbsolutePath,
        archive = archive.getAbsolutePath,
        maxFilesPerTrigger = maxFilesPerTrigger,
        trigger = "availableNow",
        deadLetter = deadLetter,
        output = output,
        process = process
      )
      .awaitTermination()

  test("AvailableNow feeds every shard row once; checkpoint prevents re-feeding on restart") {
    val base = Files.createTempDirectory("feeder").toFile
    val inbox = new File(base, "inbox"); inbox.mkdirs()
    val archive = new File(base, "archive")
    val checkpoint = new File(base, "ckpt")

    writeShards(
      inbox,
      Seq(
        InputRecord("TESTSRC", "a", "{}"),
        InputRecord("TESTSRC", "b", "{}"),
        InputRecord("TESTSRC", "c", "{}"),
        InputRecord("TESTSRC", "d", "{}")
      ),
      files = 2
    )

    rowCount.set(0)
    feed(inbox, checkpoint, archive, countingSink)
    assert(rowCount.get() == 4L, s"all 4 shard rows fed to the sink, was ${rowCount.get()}")
    assert(countFilesRecursive(checkpoint) > 0, "checkpoint advanced (exactly-once file tracking)")

    // Restart against the SAME inbox + checkpoint: committed files must never be re-fed. (cleanSource
    // archival is async/best-effort under AvailableNow, so re-feed prevention — not the move — is the
    // load-bearing plumbing assertion here.)
    rowCount.set(0)
    feed(inbox, checkpoint, archive, countingSink)
    assert(
      rowCount.get() == 0L,
      s"already-committed files must not be re-fed, was ${rowCount.get()}"
    )
  }

  test(
    "dead-letter + output sinks: errors and good frames persist, APPEND accumulates across batches"
  ) {
    val base = Files.createTempDirectory("feeder-dlq").toFile
    val inbox = new File(base, "inbox"); inbox.mkdirs()
    val archive = new File(base, "archive")
    val checkpoint = new File(base, "ckpt")
    val deadLetter = new File(base, "deadletter")
    val output = new File(base, "affected")

    // Two shards → with maxFilesPerTrigger=1 this is TWO micro-batches. 2 bad + 2 good, distinct ids
    // split across the 2 files. If a sink OVERWROTE, the dead-letter dir would end with 1 batch's
    // rows; APPEND ⇒ both batches accumulate.
    writeShards(
      inbox,
      Seq(
        InputRecord("TESTSRC", "bad-1", "{}"),
        InputRecord("TESTSRC", "good-1", "{}"),
        InputRecord("TESTSRC", "bad-2", "{}"),
        InputRecord("TESTSRC", "good-2", "{}")
      ),
      files = 2
    )

    feed(
      inbox,
      checkpoint,
      archive,
      splitProcess,
      deadLetter = deadLetter.getAbsolutePath,
      output = output.getAbsolutePath,
      maxFilesPerTrigger = 1 // force ≥2 micro-batches so the APPEND accumulation is exercised
    )

    val dl = spark.read.parquet(deadLetter.getAbsolutePath)
    val out = spark.read.parquet(output.getAbsolutePath)
    assert(
      dl.count() == 2L,
      s"both batches' error rows accumulated in dead-letter, was ${dl.count()}"
    )
    assert(out.count() == 2L, s"both batches' good rows accumulated in output, was ${out.count()}")
    // The dead-letter ErrorRow is self-describing (recordId/payload/category) — the DLQ contract.
    val cats = dl.collect().map(_.getAs[String]("category")).toSet
    assert(cats == Set(ErrorCategory.BadInput.name), s"error rows carry their category, was $cats")
    val ids = dl.collect().map(_.getAs[String]("recordId")).toSet
    assert(ids == Set("bad-1", "bad-2"), s"exactly the failed records were dead-lettered, was $ids")
  }

  test("unset sinks write nothing (back-compat: no dead-letter/output dirs created)") {
    val base = Files.createTempDirectory("feeder-nosink").toFile
    val inbox = new File(base, "inbox"); inbox.mkdirs()
    val archive = new File(base, "archive")
    val checkpoint = new File(base, "ckpt")
    val deadLetter = new File(base, "deadletter")
    val output = new File(base, "affected")

    writeShards(
      inbox,
      Seq(InputRecord("TESTSRC", "bad-x", "{}"), InputRecord("TESTSRC", "ok", "{}")),
      1
    )

    // deadLetter/output left "" ⇒ writeSinks must be a no-op.
    feed(inbox, checkpoint, archive, splitProcess)
    assert(!deadLetter.exists(), "no dead-letter dir when the sink is unset")
    assert(!output.exists(), "no output dir when the sink is unset")
  }

  test(
    "DeadLetterReprocess.selectReprocessable keeps transient categories, quarantines BAD_INPUT"
  ) {
    val ss = spark
    import ss.implicits._
    val rows = Seq(
      ErrorRow("TESTSRC", "bad-1", "{}", ErrorCategory.BadInput.name, "", "m", 0),
      ErrorRow("TESTSRC", "retry-1", "{}", ErrorCategory.RetryExhausted.name, "", "m", 3),
      ErrorRow("TESTSRC", "cfg-1", "{}", ErrorCategory.ConfigRelevant.name, "", "m", 1),
      ErrorRow("TESTSRC", "conf-1", "{}", ErrorCategory.ReplaceConflict.name, "", "m", 1)
    ).toDF()

    val selected = DeadLetterReprocess.selectReprocessable(rows)
    val ids = selected.collect().map(_.getAs[String]("recordId")).toSet
    assert(ids == Set("retry-1", "cfg-1", "conf-1"), s"BAD_INPUT stays quarantined, was $ids")
    // Projected back to the InputRecord columns for re-feeding.
    assert(
      selected.columns.toSet == Set("dataSource", "recordId", "payload"),
      s"reprocess frame is InputRecord-shaped, was ${selected.columns.mkString(",")}"
    )
  }
}
