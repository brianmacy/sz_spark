package com.senzing.spark.glue

import java.nio.charset.StandardCharsets

import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.SparkSession

import com.rabbitmq.client.{Channel, ConnectionFactory}
import com.senzing.spark.work.InputRecord

/**
 * Glue Stage 1: RabbitMQ → parquet inbox, ack-on-persist (write-ahead). A plain-JVM competing
 * consumer (run next to RabbitMQ, OUTSIDE the Spark cluster) that owns the AMQP channel and does
 * only fast work: buffer a batch of deliveries, persist them as a parquet shard, then — and ONLY
 * then — `basicAck` on the receiving channel.
 *
 * ⛔ Load-bearing invariant: persist-THEN-ack.
 *   - Crash after persist, before ack → RabbitMQ redelivers → duplicate parquet row → idempotent
 *     `addRecord` (on DATA_SOURCE,RECORD_ID) absorbs it downstream. Safe.
 *   - Crash before persist → unacked → redelivered. Safe.
 *   - Ack-before-persist would silently DROP records — never do it.
 *
 * The shard is written to `inbox/.tmp-<uuid>` (dot-prefixed so the streaming feeder skips it) and
 * atomically renamed to `inbox/part-<uuid>.parquet`, so the feeder never lists a half-written
 * footer. Parquet writing uses an embedded local[*] SparkSession inside this standalone process —
 * it does not run on the cluster.
 *
 * Args: `amqpUrl` (or env `SZ_AMQP_URL`), `queue` (default `senzing-rabbitmq-queue`), `inbox`,
 * `prefetch` (5000), `shardRecords` (5000), `emptyMs` (30000).
 */
object MqToParquet {

  private val DefaultQueue = "senzing-rabbitmq-queue"
  private val HeartbeatSeconds = 3600
  private val IdlePauseMs = 200L

  /**
   * Testable core: drain up to `shardRecords` deliveries via `basicGet`, persist the shard, then
   * ack the batch's delivery tags — persist strictly before ack. Returns the number of records
   * persisted+acked (0 when the queue is currently empty). The channel is caller-owned (real or a
   * mock); the same channel receives the deliveries and issues the acks.
   */
  def drainAndPersistShard(
      channel: Channel,
      queue: String,
      inbox: String,
      shardRecords: Int,
      spark: SparkSession
  ): Int = {
    val mapper = new ObjectMapper()
    val buf = ArrayBuffer.empty[(Long, InputRecord)]
    var continue = true
    while (continue && buf.size < shardRecords) {
      val resp = channel.basicGet(queue, /*autoAck=*/ false) // manual ack — the whole point
      if (resp == null) continue = false
      else {
        val body = new String(resp.getBody, StandardCharsets.UTF_8)
        buf += ((resp.getEnvelope.getDeliveryTag, toInputRecord(mapper, body)))
      }
    }
    if (buf.isEmpty) 0
    else {
      persistShard(spark, inbox, buf.iterator.map(_._2).toSeq) // persist FIRST (write-ahead)
      buf.foreach { case (tag, _) => channel.basicAck(tag, /*multiple=*/ false) } // ...THEN ack
      buf.size
    }
  }

  /**
   * DATA_SOURCE and RECORD_ID from the body (as `RecordJob.readRecords` does); the whole body is
   * the payload. A record missing either key is minted with the empty key(s) and dead-lettered as
   * BadInput at the engine seam, never silently stamped.
   */
  private def toInputRecord(mapper: ObjectMapper, body: String): InputRecord = {
    val tree = mapper.readTree(body)
    val ds = Option(tree.get("DATA_SOURCE")).map(_.asText).getOrElse("")
    val rid = Option(tree.get("RECORD_ID")).map(_.asText).getOrElse("")
    InputRecord(ds, rid, body)
  }

  /**
   * Write the shard as a flat `inbox/part-<uuid>.parquet` FILE that appears atomically (stage under
   * a dot-prefixed `inbox/.tmp-<uuid>` the feeder skips, then rename the single leaf out) — so both
   * the batch and streaming parquet readers see one whole file at once, never a half-written
   * footer. The atomic-rename mechanics live in [[ShardIo.writeSingleFile]]. A buffered batch is
   * never empty (the caller returns 0 before calling this), so a `None` leaf is a hard error.
   */
  private def persistShard(spark: SparkSession, inbox: String, records: Seq[InputRecord]): Unit = {
    import spark.implicits._
    ShardIo
      .writeSingleFile(spark, spark.createDataset(records).toDF(), inbox, prefix = "part")
      .getOrElse(throw new java.io.IOException(s"no parquet leaf written for shard under $inbox"))
  }

  def main(args: Array[String]): Unit = {
    val m = GlueArgs.parse(args)
    val amqpUrl = m.getOrElse("amqpUrl", sys.env.getOrElse("SZ_AMQP_URL", ""))
    val queue = m.getOrElse("queue", DefaultQueue)
    val inbox = m.getOrElse("inbox", "")
    val prefetch = m.getOrElse("prefetch", "5000").toInt
    val shardRecords = m.getOrElse("shardRecords", "5000").toInt
    val emptyMs = m.getOrElse("emptyMs", "30000").toLong

    val factory = new ConnectionFactory()
    factory.setUri(amqpUrl)
    factory.setRequestedHeartbeat(HeartbeatSeconds) // long heartbeat; fleet publisher uses 3600
    val connection = factory.newConnection()
    val channel = connection.createChannel()
    channel.basicQos(prefetch) // bounds unacked ≈ one persist-batch

    val spark = SparkSession
      .builder()
      .appName("sz-mq-to-parquet")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      var lastDelivery = System.currentTimeMillis()
      var running = true
      while (running) {
        val n = drainAndPersistShard(channel, queue, inbox, shardRecords, spark)
        if (n > 0) lastDelivery = System.currentTimeMillis()
        else if (System.currentTimeMillis() - lastDelivery >= emptyMs) running = false
        else Thread.sleep(IdlePauseMs)
      }
    } finally {
      try spark.stop()
      finally {
        try channel.close()
        finally connection.close()
      }
    }
  }
}
