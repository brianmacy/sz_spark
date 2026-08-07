package com.senzing.spark.glue

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import com.senzing.spark.work.InputRecord

/**
 * Glue Stage 0: JSONL → parquet, DATA_SOURCE + RECORD_ID parsed from the body, payload = whole
 * line.
 */
final class JsonlToParquetSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession
      .builder()
      .appName("sz-jsonl-to-parquet-test")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

  override def afterAll(): Unit = if (spark != null) spark.stop()

  test("parses DATA_SOURCE + RECORD_ID and round-trips JSONL lines to InputRecords in parquet") {
    val dir = Files.createTempDirectory("jsonl").toFile
    val input = new File(dir, "records.jsonl")
    val lines = Seq(
      """{"DATA_SOURCE":"TESTSRC","RECORD_ID":"r1","NAME_FULL":"Alice"}""",
      """{"DATA_SOURCE":"TESTSRC","RECORD_ID":"r2","NAME_FULL":"Bob"}"""
    )
    Files.write(input.toPath, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))
    val inbox = new File(dir, "inbox")

    JsonlToParquet.run(spark, input.getAbsolutePath, inbox.getAbsolutePath)

    val ss = spark
    import ss.implicits._
    val back = ss.read.parquet(inbox.getAbsolutePath).as[InputRecord].collect().sortBy(_.recordId)
    assert(back.length == 2, "two records written")
    assert(back.map(_.recordId).toSeq == Seq("r1", "r2"), "RECORD_ID parsed from each line")
    assert(back.forall(_.dataSource == "TESTSRC"), "DATA_SOURCE parsed from each line")
    assert(back.head.payload == lines.head, "payload is the whole JSONL line")
  }
}
