package com.senzing.spark.glue

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit coverage for [[FileToKafka.frame]] — the broker-free half of the on-prem producer: each
 * non-empty input line becomes one `value`-column row (the raw JSON body the Kafka sink writes and
 * [[KafkaSource]] reads back). The actual produce (`.write.format("kafka")`) is a thin connector
 * wrapper exercised only against a real broker (an `IntegrationTest`, like `KafkaSourceIT`).
 */
final class FileToKafkaSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession
      .builder()
      .appName("sz-file-to-kafka-test")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

  override def afterAll(): Unit = if (spark != null) spark.stop()

  test("frame yields one value row per non-empty line and drops blanks") {
    val dir = Files.createTempDirectory("file-to-kafka").toFile
    val file = new java.io.File(dir, "records.jsonl")
    // A blank middle line and a trailing newline — neither should become a Kafka message.
    val body = "{\"DATA_SOURCE\":\"TESTSRC\",\"RECORD_ID\":\"1\"}\n\n" +
      "{\"DATA_SOURCE\":\"TESTSRC\",\"RECORD_ID\":\"2\"}\n"
    Files.write(file.toPath, body.getBytes(StandardCharsets.UTF_8))

    val df = FileToKafka.frame(spark, file.toString)
    assert(
      df.columns.toSet == Set("value"),
      s"single value column, was ${df.columns.mkString(",")}"
    )
    val values = df.collect().map(_.getAs[String]("value")).toSet
    assert(
      values == Set(
        "{\"DATA_SOURCE\":\"TESTSRC\",\"RECORD_ID\":\"1\"}",
        "{\"DATA_SOURCE\":\"TESTSRC\",\"RECORD_ID\":\"2\"}"
      ),
      s"blank line dropped, bodies preserved verbatim, was $values"
    )
  }
}
