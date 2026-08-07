package com.senzing.spark.glue

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit coverage for [[KafkaSource]]'s pure, broker-free logic: the count-bounded range arithmetic
 * (the operating-point guarantee that a large lag becomes many small batches, not one giant one)
 * and the `bounds` round-trip that `commit` relies on. The Kafka I/O (`boundaryOffset`,
 * `readRange`) is a thin wrapper over the connector and is exercised end-to-end only with a real
 * broker (an `IntegrationTest`, like `EngineIT`); the watermark bookkeeping `commit` feeds has its
 * own [[OffsetWatermarkSpec]].
 */
class KafkaSourceSpec extends AnyFunSuite {

  test("nextRange returns None when caught up (start == latest)") {
    assert(KafkaSource.nextRange(1000L, 1000L, 1000).isEmpty)
  }

  test("nextRange caps a full batch at recordsPerBatch") {
    assert(KafkaSource.nextRange(0L, 10000L, 1000).contains((0L, 1000L)))
    assert(KafkaSource.nextRange(5000L, 10000L, 1000).contains((5000L, 6000L)))
  }

  test("nextRange shrinks the final partial batch to the available records") {
    // Only 300 records left (latest=1300, start=1000) but batch size is 1000 ⇒ read just [1000,1300).
    assert(KafkaSource.nextRange(1000L, 1300L, 1000).contains((1000L, 1300L)))
  }

  test("bounds string round-trips through parseBounds") {
    assert(KafkaSource.parseBounds("0-1000") == ((0L, 1000L)))
    assert(KafkaSource.parseBounds("1000-2500") == ((1000L, 2500L)))
  }

  test("parseBounds rejects a malformed bounds string") {
    assertThrows[IllegalArgumentException](KafkaSource.parseBounds("nodash"))
  }

  test("resolveStart parses a numeric startingOffset without touching a broker") {
    assert(KafkaSource.resolveStart("unused:9092", "t", KafkaSource.SinglePartition, "42") == 42L)
  }
}
