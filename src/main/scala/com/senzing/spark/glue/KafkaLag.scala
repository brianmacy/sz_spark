package com.senzing.spark.glue

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * Glue: Kafka processing-progress monitor for the [[KafkaSource]] feeder — the Kafka-path analog of
 * watching a queue depth. Prints the topic high-water mark (endOffsets), the feeder's durably
 * committed offset (the [[OffsetWatermark]] checkpoint the feeder writes), the lag between them,
 * and — in loop mode — the processed records/s (Δ committed / Δt).
 *
 * ⛔ Why not `kafka-consumer-groups.sh --describe`: the feeder never commits Kafka consumer-group
 * offsets (its metadata consumer sets `enable.auto.commit=false` and only reads endOffsets), so the
 * group shows no lag. Progress must be computed from the checkpoint file vs the topic HWM — which
 * is exactly what this does, reusing [[KafkaSource.boundaryOffset]] (HWM) and
 * [[OffsetWatermark.load]] (the crash-safe primary+`.bak` reader — more correct than a naive `cat`
 * during a checkpoint roll).
 *
 * Plain-JVM (no SparkSession): the FAT jar already carries kafka-clients. Run one-shot or looping.
 *
 * Args: `bootstrapServers`, `topic`, `checkpoint` (the feeder's offset dir — same one the feeder
 * writes), `intervalMs` (0 = print once and exit; > 0 = print every `intervalMs`).
 */
object KafkaLag {

  /**
   * Processed records/s between two committed-offset samples, or 0 for the first sample / a
   * non-positive elapsed. Pure — unit-testable without a broker.
   */
  private[glue] def ratePerSec(
      prevCommitted: Long,
      committed: Long,
      elapsedMs: Long
  ): Double =
    if (prevCommitted < 0 || elapsedMs <= 0) 0.0
    else (committed - prevCommitted) * 1000.0 / elapsedMs

  // scalastyle:off println
  private def emit(msg: String): Unit = println(s"SZ_KAFKA_LAG $msg")
  // scalastyle:on println

  def main(args: Array[String]): Unit = {
    val m = GlueArgs.parse(args)
    val bootstrap = m.getOrElse("bootstrapServers", "")
    val topic = m.getOrElse("topic", "")
    val checkpoint = m.getOrElse("checkpoint", "")
    require(
      bootstrap.nonEmpty && topic.nonEmpty && checkpoint.nonEmpty,
      "bootstrapServers=, topic=, and checkpoint= are required"
    )
    val intervalMs = m.getOrElse("intervalMs", "0").toLong

    val part = KafkaSource.SinglePartition
    val cpFile = new Path(checkpoint, s"offset-$topic-$part")
    val bakFile = new Path(checkpoint, s"offset-$topic-$part.bak")
    val fs = cpFile.getFileSystem(new Configuration())

    def sample(): (Long, Long) = {
      val hwm = KafkaSource.boundaryOffset(bootstrap, topic, part, earliest = false)
      val committed = OffsetWatermark.load(fs, cpFile, bakFile).getOrElse(0L)
      (hwm, committed)
    }

    if (intervalMs <= 0) {
      val (hwm, committed) = sample()
      emit(s"hwm=$hwm committed=$committed lag=${hwm - committed}")
    } else {
      var prevCommitted = -1L
      var prevMs = 0L
      var running = true
      while (running) {
        val now = System.currentTimeMillis()
        val (hwm, committed) = sample()
        val rate = ratePerSec(prevCommitted, committed, now - prevMs)
        emit(f"hwm=$hwm committed=$committed lag=${hwm - committed} rate=$rate%.1f/s")
        prevCommitted = committed
        prevMs = now
        Thread.sleep(intervalMs)
      }
    }
  }
}
