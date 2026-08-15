package com.senzing.spark.glue

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.senzing.spark.jobs.SparkJob

/**
 * Glue: JSONL file → Kafka topic. The maintained on-prem producer that loads a corpus straight into
 * the topic [[KafkaSource]] reads — the replacement for the retired RabbitMQ→Kafka bridge
 * (`MqToKafka`). Each input line is one JSON record and becomes one Kafka message value (the raw
 * body, exactly what [[KafkaSource]] projects DATA_SOURCE / RECORD_ID from with `get_json_object`).
 * No message key ⇒ the single-partition topic keeps every record on partition 0 (the locked
 * [[KafkaSource]] design — parallelism is `minPartitions` on the read side, never Kafka
 * partitions).
 *
 * Compression: `spark.read.text` decodes `.bz2` / `.gz` via Hadoop codecs; bz2 is splittable so the
 * read parallelizes across the file.
 *
 * Large records: the producer caps (`max.request.size`, `buffer.memory`) are raised to
 * [[KafkaSource.MaxRecordBytes]] (512 MiB, RabbitMQ's `max_message_size`) so a monster Sayari
 * record is not rejected by Kafka's 1 MiB default. `acks=all` for durability. The topic/broker must
 * also allow the large size (`max.message.bytes` / `message.max.bytes`) — see docs/kafka-source.md.
 *
 * Args: `input` (JSONL path, optionally compressed), `bootstrapServers`, `topic`.
 */
object FileToKafka extends SparkJob {

  /**
   * Testable core: the non-empty lines of `input` as a single `value` column — exactly the frame
   * the Kafka sink writes. Pure Spark, no broker, so it is unit-testable. Blank lines (e.g. a
   * trailing newline) are dropped so no empty Kafka message is produced.
   */
  def frame(spark: SparkSession, input: String): DataFrame =
    spark.read
      .text(input)
      .where(col("value").isNotNull && col("value") =!= "")
      .selectExpr("CAST(value AS STRING) AS value")

  /** Stream every non-empty line of `input` to `topic` as a Kafka message value. */
  def run(spark: SparkSession, input: String, bootstrapServers: String, topic: String): Unit =
    frame(spark, input).write
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", topic)
      .option("kafka.acks", "all")
      .option("kafka.max.request.size", KafkaSource.MaxRecordBytes.toString)
      .option("kafka.buffer.memory", KafkaSource.MaxRecordBytes.toString)
      .save()

  def main(args: Array[String]): Unit = {
    val m = GlueArgs.parse(args)
    val input = m.getOrElse("input", "")
    val bootstrap = m.getOrElse("bootstrapServers", "")
    val topic = m.getOrElse("topic", "")
    require(
      input.nonEmpty && bootstrap.nonEmpty && topic.nonEmpty,
      "input=, bootstrapServers=, and topic= are required"
    )
    val spark = buildSession("sz-file-to-kafka")
    try run(spark, input, bootstrap, topic)
    finally spark.stop()
  }
}
