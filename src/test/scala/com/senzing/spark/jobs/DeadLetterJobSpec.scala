package com.senzing.spark.jobs

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import com.senzing.spark.core.SplitResult
import com.senzing.spark.glue.ShardIo
import com.senzing.spark.model.{AffectedEntityRow, ErrorRow, Op, StagingRow}
import com.senzing.spark.work.{ErrorCategory, InputRecord}
import org.apache.spark.sql.Dataset

/**
 * Coverage for [[DeadLetterJob.run]]'s bounded, convergent reprocess loop with an INJECTED re-drive
 * (no real engine). Proves: (1) the loop drains when serial re-drive clears the queue, (2) a round
 * with non-shrinking residue quarantines and stops, (3) the maxRounds cap bounds a slowly-shrinking
 * queue. The single-process guarantee (`master=local[1]` + `coalesce(1)`) is structural in `main`;
 * here the loop mechanics are what is under test.
 */
final class DeadLetterJobSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession
      .builder()
      .appName("sz-dead-letter-job-test")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

  override def afterAll(): Unit = if (spark != null) spark.stop()

  /** Write one dead-letter shard (the feeder's sink primitive) under `dir`. */
  private def writeDlq(dir: String, rows: Seq[ErrorRow]): Unit = {
    val ss = spark
    import ss.implicits._
    ShardIo.writeSingleFile(ss, rows.toDF(), dir, "de")
  }

  /**
   * A fake re-drive: on call `r` it fails the FIRST `failCounts(r)` re-fed records (sorted by
   * recordId) as REPLACE_CONFLICT and succeeds the rest; calls past the end fail none. Because each
   * round re-feeds only the prior round's failures, `failCounts` steers the residue trajectory.
   */
  private def mkFake(failCounts: IndexedSeq[Int]): Dataset[InputRecord] => SplitResult = {
    var call = 0
    (in: Dataset[InputRecord]) => {
      val ss = spark
      import ss.implicits._
      val rows = in.collect().sortBy(_.recordId)
      val k = if (call < failCounts.length) failCounts(call) else 0
      call += 1
      val (failRows, goodRows) = rows.splitAt(k)
      val errs = failRows.toSeq.map(r =>
        StagingRow.of(
          ErrorRow(
            r.dataSource,
            r.recordId,
            r.payload,
            ErrorCategory.ReplaceConflict.name,
            "",
            "conflict",
            1
          )
        )
      )
      val goods = goodRows.toSeq.map(r =>
        StagingRow.of(AffectedEntityRow(r.dataSource, r.recordId, 1L, Op.Add, "t"))
      )
      SplitResult(good = goods.toDF(), errors = errs.toDF(), unpersist = () => ())
    }
  }

  private def seed(dir: String): Unit =
    writeDlq(
      dir,
      Seq(
        ErrorRow(
          "SRC",
          "bad-1",
          "{}",
          ErrorCategory.BadInput.name,
          "",
          "m",
          0
        ), // terminal, never re-fed
        ErrorRow("SRC", "retry-1", "{}", ErrorCategory.RetryExhausted.name, "", "m", 3),
        ErrorRow("SRC", "cfg-1", "{}", ErrorCategory.ConfigRelevant.name, "", "m", 1),
        ErrorRow("SRC", "rc-1", "{}", ErrorCategory.ReplaceConflict.name, "", "m", 1)
      )
    )

  private def dirs(): (String, String, String) = {
    val base = Files.createTempDirectory("dlq-job").toFile
    (
      new java.io.File(base, "deadletter").toString,
      new java.io.File(base, "work").toString,
      new java.io.File(base, "quarantine").toString
    )
  }

  test("drains when serial re-drive clears the reprocessable queue") {
    val (dl, work, q) = dirs()
    seed(dl)
    // round 0 re-feeds the 3 reprocessable rows, fail 2; round 1 re-feeds those 2, fail 0 -> drained.
    val o = DeadLetterJob.run(
      spark,
      dl,
      work,
      q,
      output = "",
      runId = "t",
      maxRounds = 5,
      mkFake(IndexedSeq(2, 0))
    )
    assert(o.drained, "loop reports drained")
    assert(o.rounds == 2, s"took two rounds, was ${o.rounds}")
    assert(o.reFedTotal == 5L, s"re-fed 3 then 2 = 5, was ${o.reFedTotal}")
    assert(o.quarantined == 0L, s"nothing quarantined, was ${o.quarantined}")
    val fs = ShardIo.fileSystem(spark, q)
    assert(ShardIo.listShards(fs, new org.apache.hadoop.fs.Path(q)).isEmpty, "quarantine empty")
  }

  test("quarantines and stops when a round makes no progress (residue not shrinking)") {
    val (dl, work, q) = dirs()
    seed(dl)
    // fail ALL 3 re-fed -> residue (3) == swept (3) -> stuck -> quarantine + stop after round 0.
    val o = DeadLetterJob.run(
      spark,
      dl,
      work,
      q,
      output = "",
      runId = "t",
      maxRounds = 5,
      mkFake(IndexedSeq(3))
    )
    assert(!o.drained, "not drained")
    assert(o.rounds == 1, s"stopped after one round, was ${o.rounds}")
    assert(o.quarantined == 1L, s"the stuck residue shard was quarantined, was ${o.quarantined}")
    val q3 = spark.read.parquet(q).count()
    assert(q3 == 3L, s"3 stuck records in quarantine, was $q3")
  }

  test("maxRounds caps a slowly-shrinking queue and quarantines the remainder") {
    val (dl, work, q) = dirs()
    seed(dl)
    // shrink 3 -> 2 -> (cap hits at round idx 1) quarantine the 2 still failing.
    val o = DeadLetterJob.run(
      spark,
      dl,
      work,
      q,
      output = "",
      runId = "t",
      maxRounds = 2,
      mkFake(IndexedSeq(2, 2, 2))
    )
    assert(!o.drained, "not drained (capped)")
    assert(o.rounds == 2, s"stopped at the cap, was ${o.rounds}")
    assert(o.quarantined == 1L, s"remainder shard quarantined, was ${o.quarantined}")
    assert(spark.read.parquet(q).count() == 2L, "2 records quarantined at the cap")
  }
}
