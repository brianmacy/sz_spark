package com.senzing.spark.glue

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit coverage for [[KafkaLag.ratePerSec]] — the broker-free processed-records/s math. The Kafka
 * I/O (HWM via `KafkaSource.boundaryOffset`, the checkpoint read via `OffsetWatermark.load`) is a
 * thin wrapper exercised only against a real broker.
 */
final class KafkaLagSpec extends AnyFunSuite {

  test("ratePerSec returns 0 for the first sample (no previous committed offset)") {
    assert(KafkaLag.ratePerSec(prevCommitted = -1L, committed = 5000L, elapsedMs = 1000L) == 0.0)
  }

  test("ratePerSec returns 0 for a non-positive elapsed") {
    assert(KafkaLag.ratePerSec(prevCommitted = 1000L, committed = 2000L, elapsedMs = 0L) == 0.0)
  }

  test("ratePerSec computes committed delta per second") {
    // 1200 records over 2s = 600/s.
    assert(
      KafkaLag.ratePerSec(prevCommitted = 1000L, committed = 2200L, elapsedMs = 2000L) == 600.0
    )
  }
}
