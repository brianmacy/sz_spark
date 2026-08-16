package com.senzing.spark.glue

import java.nio.file.Files

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import com.senzing.spark.model.ErrorRow
import com.senzing.spark.work.ErrorCategory

/**
 * Coverage for [[DeadLetterReprocess.run]]'s idempotent archive sweep: reprocessable rows are
 * re-emitted to the re-feed inbox, the swept shards are moved out of the dead-letter dir into the
 * archive (so a second pass re-emits nothing), and terminal rows stay out of the re-feed loop. The
 * pure category filter is covered separately in [[ParquetStreamFeederSpec]].
 */
final class DeadLetterReprocessSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession
      .builder()
      .appName("sz-dead-letter-reprocess-test")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

  override def afterAll(): Unit = if (spark != null) spark.stop()

  /** Write one `de-*.parquet` dead-letter shard (the feeder's sink primitive) under `dir`. */
  private def writeShard(dir: String, rows: Seq[ErrorRow]): Unit = {
    val ss = spark // bind the var to a stable val so `.implicits._` imports cleanly
    import ss.implicits._
    ShardIo.writeSingleFile(ss, rows.toDF(), dir, "de")
  }

  private def mixedRows: Seq[ErrorRow] = Seq(
    ErrorRow("TESTSRC", "bad-1", "{}", ErrorCategory.BadInput.name, "", "m", 0),
    ErrorRow("TESTSRC", "retry-1", "{}", ErrorCategory.RetryExhausted.name, "", "m", 3),
    ErrorRow("TESTSRC", "cfg-1", "{}", ErrorCategory.ConfigRelevant.name, "", "m", 1)
  )

  test("run re-emits only reprocessable rows, archives swept shards, and is idempotent") {
    val base = Files.createTempDirectory("dlq-reprocess").toFile
    val deadLetter = new java.io.File(base, "deadletter").toString
    val reFeed = new java.io.File(base, "refeed").toString
    val archive = new java.io.File(base, "archived").toString

    writeShard(deadLetter, mixedRows)
    writeShard(deadLetter, mixedRows) // two shards to prove the whole snapshot is swept

    val fs = ShardIo.fileSystem(spark, deadLetter)
    assert(ShardIo.listShards(fs, new Path(deadLetter)).length == 2, "two shards staged")

    // First pass: 2 reprocessable per shard × 2 shards = 4 re-emitted (bad-1 quarantined).
    val n1 = DeadLetterReprocess.run(spark, deadLetter, reFeed, archive)
    assert(n1 == 4, s"re-emitted the reprocessable rows, was $n1")

    val ids = spark.read.parquet(reFeed).collect().map(_.getAs[String]("recordId")).toSet
    assert(ids == Set("retry-1", "cfg-1"), s"only transient categories re-fed, was $ids")
    assert(
      spark.read.parquet(reFeed).columns.toSet == Set("dataSource", "recordId", "payload"),
      "re-feed shards are InputRecord-shaped"
    )
    assert(
      ShardIo.listShards(fs, new Path(deadLetter)).isEmpty,
      "swept shards left the dead-letter dir"
    )
    assert(
      ShardIo.listShards(fs, new Path(archive)).length == 2,
      "swept shards landed in the archive"
    )

    // Second pass over the now-empty dir: nothing new (idempotent — the prior bug re-emitted forever).
    val before = spark.read.parquet(reFeed).count()
    val n2 = DeadLetterReprocess.run(spark, deadLetter, reFeed, archive)
    assert(n2 == 0, s"second pass re-emits nothing, was $n2")
    assert(spark.read.parquet(reFeed).count() == before, "re-feed dir unchanged by the empty pass")
  }

  test("run is a 0-count no-op on a missing dead-letter dir") {
    val base = Files.createTempDirectory("dlq-empty").toFile
    val deadLetter = new java.io.File(base, "absent").toString
    val reFeed = new java.io.File(base, "refeed").toString
    val archive = new java.io.File(base, "archived").toString
    assert(DeadLetterReprocess.run(spark, deadLetter, reFeed, archive) == 0L)
  }
}
