package com.senzing.spark.glue

import org.scalatest.funsuite.AnyFunSuite

/**
 * Pure, broker-free coverage of [[DeltaSource]]'s VERSION-window arithmetic — the version cursor is
 * inclusive of `latest` (a Delta table's current version is a real, readable version), unlike
 * Kafka's exclusive next-offset, so the boundary handling differs and is worth pinning. The CDF
 * read and `latestVersion` lookup need a real Delta table and are exercised in an
 * `IntegrationTest`.
 */
final class DeltaSourceSpec extends AnyFunSuite {

  test("nextRange returns None only when the cursor is past the latest version") {
    assert(DeltaSource.nextRange(6L, 5L, 1).isEmpty) // caught up: nothing at/after version 6
    assert(DeltaSource.nextRange(5L, 5L, 1).contains((5L, 6L))) // version 5 IS readable
  }

  test("nextRange advances one version at a time by default (exclusive-upper cursor)") {
    assert(DeltaSource.nextRange(0L, 10L, 1).contains((0L, 1L))) // read v0, next cursor 1
    assert(DeltaSource.nextRange(3L, 10L, 1).contains((3L, 4L)))
  }

  test("nextRange caps a window at versionsPerBatch and clamps to latest+1") {
    assert(DeltaSource.nextRange(0L, 10L, 4).contains((0L, 4L))) // v0..v3
    assert(DeltaSource.nextRange(8L, 10L, 4).contains((8L, 11L))) // only v8,v9,v10 remain
  }

  test("commit bounds share KafkaSource's start-end encoding") {
    assert(KafkaSource.parseBounds("8-11") == ((8L, 11L)))
  }
}
