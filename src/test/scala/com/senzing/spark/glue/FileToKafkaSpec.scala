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

  test("shouldThrottle: true iff the feeder is at least maxLag behind the Kafka tail") {
    // hwm - committed >= maxLag ⇒ pause producing.
    assert(FileToKafka.shouldThrottle(hwm = 5_000_000L, committed = 0L, maxLag = 5_000_000L))
    assert(FileToKafka.shouldThrottle(hwm = 6_000_000L, committed = 500_000L, maxLag = 5_000_000L))
    // Just under the cap ⇒ keep producing.
    assert(!FileToKafka.shouldThrottle(hwm = 4_999_999L, committed = 0L, maxLag = 5_000_000L))
    // Feeder caught up ⇒ keep producing.
    assert(
      !FileToKafka.shouldThrottle(hwm = 5_000_000L, committed = 4_999_999L, maxLag = 5_000_000L)
    )
    // Empty topic ⇒ keep producing.
    assert(!FileToKafka.shouldThrottle(hwm = 0L, committed = 0L, maxLag = 5_000_000L))
  }

  test("producible: only non-empty, non-null lines become Kafka messages") {
    assert(FileToKafka.producible("{\"RECORD_ID\":\"1\"}"))
    assert(!FileToKafka.producible(""))
    assert(!FileToKafka.producible(null))
  }

  test("shouldProduce: skip then shard is disjoint + gapless across the union of shards") {
    val skip = 100L
    val shards = 2
    // Everything below the resume point is skipped by BOTH shards.
    assert(!FileToKafka.shouldProduce(gidx = 0L, skip, shards, shardIndex = 0))
    assert(!FileToKafka.shouldProduce(gidx = 99L, skip, shards, shardIndex = 1))
    // At/after the resume point, every index is owned by EXACTLY ONE shard (partition of the tail).
    for (gidx <- 100L to 130L) {
      val owned = (0 until shards).count(s => FileToKafka.shouldProduce(gidx, skip, shards, s))
      assert(owned == 1, s"gidx=$gidx owned by $owned shards, expected exactly 1")
    }
    // Concretely: even index → shard 0, odd → shard 1.
    assert(FileToKafka.shouldProduce(gidx = 100L, skip, shards, shardIndex = 0))
    assert(!FileToKafka.shouldProduce(gidx = 100L, skip, shards, shardIndex = 1))
    assert(FileToKafka.shouldProduce(gidx = 101L, skip, shards, shardIndex = 1))
    // shards=1 ⇒ single producer owns the whole tail.
    assert(FileToKafka.shouldProduce(gidx = 100L, skip, shards = 1, shardIndex = 0))
  }
}
