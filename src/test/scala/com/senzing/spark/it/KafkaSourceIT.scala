package com.senzing.spark.it

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.{Properties, UUID}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import com.senzing.spark.IntegrationTest
import com.senzing.spark.glue.{KafkaSource, OffsetWatermark}

/**
 * End-to-end [[KafkaSource]] against a REAL broker. Tagged [[IntegrationTest]] so the default
 * `sbt test` (and CI) EXCLUDES it; run with a live Kafka:
 * `SZ_IT=1 SZ_KAFKA_BOOTSTRAP=broker:9092 sbt "testOnly *KafkaSourceIT -- -n com.senzing.spark.IntegrationTest"`.
 *
 * Produces N records to a fresh single-partition topic, then drives `KafkaSource` +
 * `OffsetWatermark` and asserts: the earliest/latest boundary offsets, the count-bounded ranges
 * (2500 records / 1000 per batch ⇒ 1000, 1000, 500), the DATA_SOURCE/RECORD_ID projection from the
 * body, and that the contiguous-prefix watermark advances to the tail after all commits.
 */
final class KafkaSourceIT extends AnyFunSuite {

  private def enabled: Boolean =
    sys.env.get("SZ_IT").contains("1") && sys.env.get("SZ_KAFKA_BOOTSTRAP").exists(_.nonEmpty)

  test("KafkaSource reads count-bounded ranges and advances the watermark", IntegrationTest) {
    assume(enabled, "requires SZ_IT=1 + SZ_KAFKA_BOOTSTRAP=<broker> + a live Kafka")
    val bootstrap = sys.env("SZ_KAFKA_BOOTSTRAP")
    val topic = "sz-kafka-it-" + UUID.randomUUID().toString.take(8)
    val total = 2500
    val batch = 1000

    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("key.serializer", classOf[ByteArraySerializer].getName)
    props.put("value.serializer", classOf[ByteArraySerializer].getName)
    props.put("acks", "all")
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
    try {
      (0 until total).foreach { i =>
        val body =
          s"""{"DATA_SOURCE":"TESTSRC","RECORD_ID":"R$i"}""".getBytes(StandardCharsets.UTF_8)
        producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, null, body))
      }
      producer.flush()
    } finally producer.close()

    val spark = SparkSession
      .builder()
      .appName("sz-kafka-it")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    val cpDir = "file://" + Files.createTempDirectory("sz-kafka-it-cp").toString
    val fs: FileSystem = FileSystem.getLocal(new Configuration())
    val cpFile = new Path(cpDir, s"offset-$topic-0")
    try {
      assert(KafkaSource.boundaryOffset(bootstrap, topic, 0, earliest = true) == 0L)
      assert(KafkaSource.boundaryOffset(bootstrap, topic, 0, earliest = false) == total.toLong)

      val wm = new OffsetWatermark(fs, cpFile, start = 0L)
      val source =
        new KafkaSource(spark, bootstrap, topic, wm, recordsPerBatch = batch, minPartitions = 1)

      var cursor = source.initialCursor
      var read = 0L
      var chunks = 0
      var done = false
      while (!done) {
        source.nextChunk(cursor) match {
          case Some(c) =>
            val rows = c.df.collect()
            read += rows.length
            chunks += 1
            assert(rows.forall(_.recordId.startsWith("R")), "RECORD_ID projected from the body")
            assert(rows.forall(_.dataSource == "TESTSRC"), "DATA_SOURCE projected from the body")
            source.commit(c.bounds)
            cursor = c.nextCursor
          case None => done = true
        }
      }
      assert(read == total.toLong, s"read all $total records, got $read")
      assert(chunks == 3, "2500 records / 1000 per batch ⇒ 3 count-bounded chunks")
      assert(wm.committedOffset == total.toLong, "watermark advanced to the tail after all commits")
    } finally {
      spark.stop()
      fs.close()
    }
  }
}
