package com.senzing.spark.glue

import java.util.Properties
import java.util.concurrent.Future

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.ByteArraySerializer

import com.rabbitmq.client.{Channel, ConnectionFactory}

/**
 * Glue Step 2b: the RabbitMQ → Kafka bridge — a plain-JVM competing consumer that moves records
 * from the RabbitMQ queue onto the Kafka topic [[KafkaSource]] reads, THROTTLED so the Spark
 * consumer never falls more than `maxLag` records behind (Kafka is the durable buffer between
 * RabbitMQ and Spark). The RabbitMQ→Kafka analog of the [[MqToParquet]] drainer→parquet seam.
 *
 * ⛔ Load-bearing invariant: produce-THEN-ack (write-ahead, exactly like [[MqToParquet]]'s
 * persist-then-ack).
 *   - Crash after produce, before ack → RabbitMQ redelivers → duplicate Kafka record → idempotent
 *     `addRecord` (on DATA_SOURCE,RECORD_ID) absorbs it downstream. Safe.
 *   - Crash before produce → unacked → redelivered. Safe.
 *   - Ack-before-produce would silently DROP records — never do it.
 *
 * THROTTLE: `lag = (Kafka latest offset) − (the feeder's committed offset)`, the committed offset
 * read from the SAME checkpoint [[KafkaSource]]/[[OffsetWatermark]] persist. While `lag ≥ maxLag`
 * the bridge pauses draining, so unread Kafka records stay bounded (the analog of the queue's
 * reject-publish cap) — Kafka retention + this cap replace unbounded queue growth.
 *
 * Args: `amqpUrl` (or env `SZ_AMQP_URL`), `queue` (default `senzing-rabbitmq-queue`),
 * `bootstrapServers`, `topic`, `checkpoint` (the feeder's offset dir), `maxLag` (5,000,000),
 * `batchRecords` (5000), `emptyMs` (30000).
 */
object MqToKafka {

  private val DefaultQueue = "senzing-rabbitmq-queue"
  private val DefaultMaxLag = 5000000L
  private val HeartbeatSeconds = 3600
  private val IdlePauseMs = 200L
  private val ThrottlePauseMs = 1000L
  // Match RabbitMQ's max_message_size (512 MiB) so any record RabbitMQ accepts also flows through
  // Kafka. The 1 MiB producer default (max.request.size) rejected large Sayari records
  // (RecordTooLargeException) and crashed the bridge. Shared with the consumer side (KafkaSource).
  val MaxRecordBytes = 536870912L // 512 MiB == RabbitMQ max_message_size

  /** True when the Spark consumer is at least `maxLag` records behind the Kafka tail. */
  private[glue] def shouldThrottle(latest: Long, committed: Long, maxLag: Long): Boolean =
    latest - committed >= maxLag

  /**
   * Testable core: drain up to `batchRecords` deliveries via `basicGet`, produce each to `topic`,
   * block until EVERY send is durably acknowledged, THEN ack the batch's delivery tags — produce
   * strictly before ack. Returns the number produced+acked (0 when the queue is currently empty). A
   * send failure throws from `future.get()` BEFORE any ack, so nothing is acked and RabbitMQ
   * redelivers the whole batch (at-least-once). The channel and producer are caller-owned (real or
   * a test double); the Kafka `value` is the raw RabbitMQ body (identical to what [[KafkaSource]]
   * reads).
   */
  def drainAndProduce(
      channel: Channel,
      queue: String,
      producer: Producer[Array[Byte], Array[Byte]],
      topic: String,
      batchRecords: Int
  ): Int = {
    val tags = ArrayBuffer.empty[Long]
    val futures = ArrayBuffer.empty[Future[RecordMetadata]]
    var continue = true
    while (continue && tags.size < batchRecords) {
      val resp = channel.basicGet(queue, /*autoAck=*/ false) // manual ack — the whole point
      if (resp == null) continue = false
      else {
        futures += producer.send(
          new ProducerRecord[Array[Byte], Array[Byte]](topic, null, resp.getBody)
        )
        tags += resp.getEnvelope.getDeliveryTag
      }
    }
    if (tags.isEmpty) 0
    else {
      producer.flush() // push the batch
      futures.foreach(_.get()) // block until durably acked; a failure throws BEFORE any ack
      tags.foreach(tag => channel.basicAck(tag, /*multiple=*/ false)) // ...THEN ack
      tags.size
    }
  }

  def main(args: Array[String]): Unit = {
    val m = GlueArgs.parse(args)
    val amqpUrl = m.getOrElse("amqpUrl", sys.env.getOrElse("SZ_AMQP_URL", ""))
    val queue = m.getOrElse("queue", DefaultQueue)
    val bootstrap = m.getOrElse("bootstrapServers", "")
    val topic = m.getOrElse("topic", "")
    val checkpoint = m.getOrElse("checkpoint", "")
    require(
      amqpUrl.nonEmpty && bootstrap.nonEmpty && topic.nonEmpty && checkpoint.nonEmpty,
      "amqpUrl (or SZ_AMQP_URL), bootstrapServers=, topic=, and checkpoint= are required"
    )
    val maxLag = m.getOrElse("maxLag", DefaultMaxLag.toString).toLong
    val batchRecords = m.getOrElse("batchRecords", "5000").toInt
    val emptyMs = m.getOrElse("emptyMs", "30000").toLong

    val factory = new ConnectionFactory()
    factory.setUri(amqpUrl)
    factory.setRequestedHeartbeat(HeartbeatSeconds)
    val connection = factory.newConnection()
    val channel = connection.createChannel()
    channel.basicQos(batchRecords) // bounds unacked ≈ one produce-batch

    val producer = newProducer(bootstrap)

    // The feeder's committed offset lives in the same checkpoint dir KafkaSource writes.
    val cpFile = new Path(checkpoint, s"offset-$topic-${KafkaSource.SinglePartition}")
    val bakFile = new Path(checkpoint, s"offset-$topic-${KafkaSource.SinglePartition}.bak")
    val fs = cpFile.getFileSystem(new Configuration())

    try {
      var lastDelivery = System.currentTimeMillis()
      var running = true
      while (running) {
        val latest = KafkaSource.boundaryOffset(
          bootstrap,
          topic,
          KafkaSource.SinglePartition,
          earliest = false
        )
        val committed = OffsetWatermark.load(fs, cpFile, bakFile).getOrElse(0L)
        if (shouldThrottle(latest, committed, maxLag)) Thread.sleep(ThrottlePauseMs)
        else {
          val n = drainAndProduce(channel, queue, producer, topic, batchRecords)
          if (n > 0) lastDelivery = System.currentTimeMillis()
          else if (System.currentTimeMillis() - lastDelivery >= emptyMs) running = false
          else Thread.sleep(IdlePauseMs)
        }
      }
    } finally {
      try producer.close()
      finally {
        try channel.close()
        finally connection.close()
      }
    }
  }

  private def newProducer(bootstrap: String): Producer[Array[Byte], Array[Byte]] = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("key.serializer", classOf[ByteArraySerializer].getName)
    props.put("value.serializer", classOf[ByteArraySerializer].getName)
    props.put("acks", "all") // durable before we ack RabbitMQ
    props.put("enable.idempotence", "true") // no duplicate on producer retry
    // Match RabbitMQ (512 MiB): permit large records instead of the 1 MiB default that crashed the
    // bridge. buffer.memory must be >= max.request.size so one max-size record can be buffered.
    props.put("max.request.size", MaxRecordBytes.toString)
    props.put("buffer.memory", MaxRecordBytes.toString)
    new KafkaProducer[Array[Byte], Array[Byte]](props)
  }
}
