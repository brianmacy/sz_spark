package com.senzing.spark.glue

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
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
 * TWO MODES:
 *
 *   1. Simple batch ([[run]]) — `spark.read.text(input)` → `.write.format("kafka")`. Fire-hose: it
 *      produces the whole file as fast as the cluster allows. Fine when the topic can hold the
 *      entire corpus (Kafka retention + broker disk cover it) and there is no resume point.
 *   2. Throttled + resumable ([[runThrottled]]) — a DRIVER-SIDE SEQUENTIAL producer that (a) skips
 *      the first `skipRecords` lines so a cut-over resumes where a prior load stopped instead of
 *      re-adding what is already resolved, and (b) keeps Kafka bounded: while the feeder is
 *      `≥ maxLag` records behind the topic tail it pauses producing (the same
 *      `lag = HWM − committedOffset` throttle the retired bridge used, reusing
 *      [[KafkaSource.boundaryOffset]] + [[OffsetWatermark.load]] on the SHARED checkpoint the
 *      feeder writes). This is required when the whole corpus does not fit on the broker's disk —
 *      Kafka only ever holds ~`maxLag` records instead of the entire file.
 *
 * The batch write cannot throttle (`.save()` is one atomic write) and Spark's read does not
 * preserve file order across a splittable `.bz2`, so the resume/skip and the throttle both live in
 * the sequential driver-side mode. Compression (`.bz2` / `.gz`) is decoded via Hadoop's
 * [[CompressionCodecFactory]] (present because these jobs launch under `spark-submit`).
 *
 * Large records: the producer caps (`max.request.size`, `buffer.memory`) are raised to
 * [[KafkaSource.MaxRecordBytes]] (512 MiB, RabbitMQ's `max_message_size`) so a monster Sayari
 * record is not rejected by Kafka's 1 MiB default. `acks=all` for durability. The topic/broker must
 * also allow the large size (`max.message.bytes` / `message.max.bytes`) — see docs/kafka-source.md.
 *
 * Args: `input` (JSONL path, optionally compressed), `bootstrapServers`, `topic`. For the throttled
 * mode also `checkpoint` (the feeder's offset dir — its presence selects throttled mode), and
 * optionally `maxLag` (default 5,000,000), `skipRecords` (default 0), `shards` (default 1) +
 * `shardIndex` (default 0) for multi-topic/multi-feeder throughput, `checkEvery` (default 50,000),
 * `throttlePauseMs` (default 1,000).
 */
object FileToKafka extends SparkJob {

  /**
   * Default: keep Kafka within this many records of the feeder — mirrors the bridge / KafkaSource.
   */
  val DefaultMaxLag: Long = 5000000L

  /** How many produced records between throttle checks (bounds boundaryOffset consumer churn). */
  val DefaultCheckEvery: Long = 50000L

  /** Sleep between throttle re-checks while the feeder is ≥ maxLag behind. */
  val DefaultThrottlePauseMs: Long = 1000L

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

  /** Batch mode: stream every non-empty line of `input` to `topic` as a Kafka message value. */
  def run(spark: SparkSession, input: String, bootstrapServers: String, topic: String): Unit =
    frame(spark, input).write
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", topic)
      .option("kafka.acks", "all")
      .option(
        "kafka.compression.type",
        "zstd"
      ) // producer-side compression — the durable fixture topic
      .option("kafka.max.request.size", KafkaSource.MaxRecordBytes.toString)
      .option("kafka.buffer.memory", KafkaSource.MaxRecordBytes.toString)
      .save()

  /**
   * True when the feeder is at least `maxLag` records behind the Kafka tail. Pure — unit-testable.
   */
  private[glue] def shouldThrottle(hwm: Long, committed: Long, maxLag: Long): Boolean =
    hwm - committed >= maxLag

  /** A line becomes a Kafka message iff it is non-empty (mirrors [[frame]]'s blank-line drop). */
  private[glue] def producible(line: String): Boolean = line != null && line.nonEmpty

  /**
   * Whether THIS producer emits the producible line at global index `gidx` (0-based over all
   * producible lines). Resume: skip the first `skipRecords`. Shard: of the remainder, take only the
   * lines this shard owns (`gidx % shards == shardIndex`). Two producers over the SAME corpus with
   * the same `shards`/`skipRecords` but distinct `shardIndex` emit DISJOINT sets whose union is
   * every line `≥ skipRecords` — no gap, no overlap, each feeding its own single-partition topic.
   * Pure — unit-testable.
   */
  private[glue] def shouldProduce(
      gidx: Long,
      skipRecords: Long,
      shards: Int,
      shardIndex: Int
  ): Boolean =
    gidx >= skipRecords && (gidx % shards == shardIndex)

  private def newProducer(bootstrap: String): Producer[Array[Byte], Array[Byte]] = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("acks", "all")
    props.put("compression.type", "zstd") // producer-side compression — the durable fixture topic
    // A monster Sayari record can approach the RabbitMQ 512 MiB ceiling; the 1 MiB producer default
    // (max.request.size) would reject it. buffer.memory must be >= max.request.size so one max-size
    // record can be buffered.
    props.put("max.request.size", KafkaSource.MaxRecordBytes.toString)
    props.put("buffer.memory", KafkaSource.MaxRecordBytes.toString)
    new KafkaProducer[Array[Byte], Array[Byte]](props)
  }

  /**
   * Throttled + resumable + shardable driver-side sequential producer. Emits the producible lines
   * this shard owns (see [[shouldProduce]]: skip the first `skipRecords`, then take
   * `gidx % shards == shardIndex`) to `topic`, pausing while the feeder is ≥ `maxLag` records
   * behind (so Kafka stays bounded and the whole corpus need not fit on the broker's disk). Run one
   * instance per `shardIndex`, each to its own single-partition topic + checkpoint, for
   * multi-feeder throughput (KafkaSource is single-partition/single-feeder, so parallelism is one
   * topic per host).
   */
  def runThrottled(
      spark: SparkSession,
      input: String,
      bootstrapServers: String,
      topic: String,
      checkpoint: String,
      maxLag: Long = DefaultMaxLag,
      skipRecords: Long = 0L,
      shards: Int = 1,
      shardIndex: Int = 0,
      checkEvery: Long = DefaultCheckEvery,
      throttlePauseMs: Long = DefaultThrottlePauseMs
  ): Unit = {
    require(shards >= 1, s"shards must be >= 1, was $shards")
    require(
      shardIndex >= 0 && shardIndex < shards,
      s"shardIndex must be in [0,$shards), was $shardIndex"
    )
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val part = KafkaSource.SinglePartition
    // The feeder's committed offset lives in the same checkpoint dir KafkaSource/OffsetWatermark use.
    val cpFile = new Path(checkpoint, s"offset-$topic-$part")
    val bakFile = new Path(checkpoint, s"offset-$topic-$part.bak")
    val cpFs = cpFile.getFileSystem(new Configuration())

    val inPath = new Path(input)
    val inFs = inPath.getFileSystem(hadoopConf)
    val codec = new CompressionCodecFactory(hadoopConf).getCodec(inPath)
    val rawIn = inFs.open(inPath)
    val in = if (codec != null) codec.createInputStream(rawIn) else rawIn
    val reader = new BufferedReader(new InputStreamReader(in, UTF_8))
    val producer = newProducer(bootstrapServers)

    def waitForCapacity(): Unit = {
      producer.flush() // so the HWM reflects everything already sent before we measure lag
      var hwm = KafkaSource.boundaryOffset(bootstrapServers, topic, part, earliest = false)
      var committed = OffsetWatermark.load(cpFs, cpFile, bakFile).getOrElse(0L)
      while (shouldThrottle(hwm, committed, maxLag)) {
        Thread.sleep(throttlePauseMs)
        hwm = KafkaSource.boundaryOffset(bootstrapServers, topic, part, earliest = false)
        committed = OffsetWatermark.load(cpFs, cpFile, bakFile).getOrElse(0L)
      }
    }

    try {
      emit(
        s"start: topic=$topic shard=$shardIndex/$shards skipRecords=$skipRecords maxLag=$maxLag"
      )
      // gidx = global 0-based index over ALL producible lines (identical across shards, so the
      // shard split is disjoint + gapless). Skip + shard selection both key off gidx.
      var gidx = 0L
      var produced = 0L
      var line = reader.readLine()
      while (line != null) {
        if (producible(line)) {
          if (shouldProduce(gidx, skipRecords, shards, shardIndex)) {
            producer.send(
              new ProducerRecord[Array[Byte], Array[Byte]](topic, null, line.getBytes(UTF_8))
            )
            produced += 1
            if (produced % checkEvery == 0) {
              waitForCapacity()
              emit(s"produced=$produced gidx=$gidx (throttled to <= $maxLag ahead of feeder)")
            }
          }
          gidx += 1
        }
        line = reader.readLine()
      }
      producer.flush()
      emit(s"done: produced $produced of $gidx producible record(s) to $topic")
    } finally {
      producer.close()
      reader.close()
    }
  }

  // scalastyle:off println
  private def emit(msg: String): Unit = println(s"SZ_FILE_TO_KAFKA $msg")
  // scalastyle:on println

  def main(args: Array[String]): Unit = {
    val m = GlueArgs.parse(args)
    val input = m.getOrElse("input", "")
    val bootstrap = m.getOrElse("bootstrapServers", "")
    val topic = m.getOrElse("topic", "")
    val checkpoint = m.getOrElse("checkpoint", "")
    require(
      input.nonEmpty && bootstrap.nonEmpty && topic.nonEmpty,
      "input=, bootstrapServers=, and topic= are required"
    )
    val spark = buildSession("sz-file-to-kafka")
    try
      // `checkpoint=` present ⇒ throttled + resumable (bounded Kafka); absent ⇒ simple batch.
      if (checkpoint.nonEmpty) {
        val maxLag = m.getOrElse("maxLag", DefaultMaxLag.toString).toLong
        val skipRecords = m.getOrElse("skipRecords", "0").toLong
        val shards = m.getOrElse("shards", "1").toInt
        val shardIndex = m.getOrElse("shardIndex", "0").toInt
        val checkEvery = m.getOrElse("checkEvery", DefaultCheckEvery.toString).toLong
        val throttlePauseMs = m.getOrElse("throttlePauseMs", DefaultThrottlePauseMs.toString).toLong
        runThrottled(
          spark,
          input,
          bootstrap,
          topic,
          checkpoint,
          maxLag,
          skipRecords,
          shards,
          shardIndex,
          checkEvery,
          throttlePauseMs
        )
      } else run(spark, input, bootstrap, topic)
    finally spark.stop()
  }
}
