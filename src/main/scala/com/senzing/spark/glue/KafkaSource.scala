package com.senzing.spark.glue

import java.time.Duration
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.{Dataset, SparkSession}

import com.senzing.spark.work.InputRecord

/**
 * Monotonic-watermark [[RecordSource]] over ONE Kafka topic — the object-store-safe,
 * Databricks-native counterpart to the on-prem dispose-flavor [[InboxSource]]. Same
 * [[OverlappingBatchEngine]], only the source differs (Step 2 of the parallel-batch feeder).
 *
 * DESIGN (locked): ONE unpartitioned topic (partition 0). Spark read parallelism comes from
 * `minPartitions` fanning that single Kafka partition into N read tasks — NOT from Kafka
 * partitions, so records are never grouped by a resolution key (which would create the cross-key
 * entity-lock contention the project deliberately avoids). The cursor is a single offset; the
 * durable, out-of-order-safe contiguous-prefix bookkeeping lives in [[OffsetWatermark]].
 *
 *   - `nextChunk(cursor)` claims `[cursor, min(cursor + recordsPerBatch, latest))` as a LAZY Kafka
 *     batch read; returns `None` when `cursor == latest` (caught up — the engine idles). `bounds` =
 *     `"start-end"`, `nextCursor` = end. Bounding by a record COUNT (not "read to latest") is what
 *     keeps the 1-partition-per-batch operating point: a large lag becomes many small batches, not
 *     one giant straggler-prone batch.
 *   - `commit("start-end")` advances the [[OffsetWatermark]] over the contiguous-completed prefix.
 *   - `reclaim()` is a no-op — a restart re-reads from the committed offset; replaying the
 *     completed-but-behind-a-straggler tail is a handful of cheap optimized no-op re-adds.
 *
 * The Spark Kafka connector (`spark-sql-kafka-0-10`) is `Provided` — supply it at launch with
 * `--packages org.apache.spark:spark-sql-kafka-0-10_2.13:<sparkVersion>` (or it is present on
 * Databricks). See `docs/PARALLEL_BATCH_FEEDER.md` §Step 2.
 */
final class KafkaSource(
    spark: SparkSession,
    bootstrapServers: String,
    topic: String,
    watermark: OffsetWatermark,
    recordsPerBatch: Int,
    minPartitions: Int
) extends RecordSource {

  require(recordsPerBatch > 0, s"recordsPerBatch must be > 0, was $recordsPerBatch")
  require(minPartitions > 0, s"minPartitions must be > 0, was $minPartitions")

  private val partition = KafkaSource.SinglePartition

  def initialCursor: String = watermark.committedOffset.toString

  def nextChunk(cursor: String): Option[Chunk] = {
    val start = cursor.toLong
    val latest = KafkaSource.boundaryOffset(bootstrapServers, topic, partition, earliest = false)
    KafkaSource.nextRange(start, latest, recordsPerBatch).map { case (s, e) =>
      Chunk(bounds = s"$s-$e", df = readRange(s, e), nextCursor = e.toString)
    }
  }

  def commit(bounds: String): Unit = {
    val (start, end) = KafkaSource.parseBounds(bounds)
    watermark.complete(start, end)
  }

  def reclaim(): Unit = () // watermark flavor: restart re-reads from the committed offset

  /**
   * Lazy bounded read of `[start, end)` (Spark `endingOffsets` are EXCLUSIVE) mapped to
   * InputRecord. The Kafka `value` is the raw JSON body (identical to the RabbitMQ path);
   * DATA_SOURCE / RECORD_ID are projected on the executors with `get_json_object` (no driver-side
   * Jackson bottleneck), and the whole body is the payload.
   */
  private def readRange(start: Long, end: Long): Dataset[InputRecord] = {
    import spark.implicits._
    val startJson = s"""{"$topic":{"$partition":$start}}"""
    val endJson = s"""{"$topic":{"$partition":$end}}"""
    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", startJson)
      .option("endingOffsets", endJson)
      .option("minPartitions", minPartitions)
      .load()
      .selectExpr("CAST(value AS STRING) AS body")
      .selectExpr(
        "COALESCE(GET_JSON_OBJECT(body, '$.DATA_SOURCE'), '') AS dataSource",
        "COALESCE(GET_JSON_OBJECT(body, '$.RECORD_ID'), '') AS recordId",
        "body AS payload"
      )
      .as[InputRecord]
  }
}

object KafkaSource {

  /**
   * One unpartitioned topic ⇒ a single Kafka partition; parallelism is `minPartitions`, not this.
   */
  val SinglePartition: Int = 0

  /**
   * The next bounded range to claim from `start` given the current `latest` (exclusive) offset:
   * `None` when caught up (`start >= latest`), else
   * `[start, min(start + recordsPerBatch, latest))`. Bounding by a record COUNT — never "read to
   * latest" — is what preserves the 1-partition-per-batch operating point: a large lag is split
   * into many small batches, not one straggler-prone giant one.
   */
  private[glue] def nextRange(
      start: Long,
      latest: Long,
      recordsPerBatch: Int
  ): Option[(Long, Long)] =
    if (start >= latest) None
    else Some((start, math.min(start + recordsPerBatch, latest)))

  /** Parse a `"start-end"` bounds string back to the offset pair. */
  private[glue] def parseBounds(bounds: String): (Long, Long) = {
    val dash = bounds.lastIndexOf('-')
    require(dash > 0, s"malformed kafka bounds '$bounds' (expected start-end)")
    (bounds.substring(0, dash).toLong, bounds.substring(dash + 1).toLong)
  }

  /**
   * Driver-side metadata: the beginning (earliest) or end (latest, EXCLUSIVE high bound) offset for
   * one topic-partition. Used to bound each batch to the records actually available.
   */
  def boundaryOffset(
      bootstrapServers: String,
      topic: String,
      partition: Int,
      earliest: Boolean
  ): Long = {
    val consumer = newMetaConsumer(bootstrapServers, topic)
    try {
      val tp = new TopicPartition(topic, partition)
      consumer.assign(Collections.singletonList(tp))
      val offsets =
        if (earliest) consumer.beginningOffsets(Collections.singletonList(tp))
        else consumer.endOffsets(Collections.singletonList(tp))
      offsets.get(tp).longValue()
    } finally consumer.close(Duration.ofSeconds(10))
  }

  /** Resolve a `startingOffset` spec (`earliest` | `latest` | a number) to a concrete offset. */
  def resolveStart(bootstrapServers: String, topic: String, partition: Int, spec: String): Long =
    spec.trim.toLowerCase match {
      case "earliest" => boundaryOffset(bootstrapServers, topic, partition, earliest = true)
      case "latest" => boundaryOffset(bootstrapServers, topic, partition, earliest = false)
      case n => n.toLong
    }

  private def newMetaConsumer(
      bootstrapServers: String,
      topic: String
  ): KafkaConsumer[Array[Byte], Array[Byte]] = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.deserializer", classOf[ByteArrayDeserializer].getName)
    props.put("value.deserializer", classOf[ByteArrayDeserializer].getName)
    props.put("group.id", s"sz-spark-kafkasource-meta-$topic")
    props.put("enable.auto.commit", "false")
    new KafkaConsumer[Array[Byte], Array[Byte]](props)
  }
}
