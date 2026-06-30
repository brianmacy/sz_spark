package com.senzing.spark.jobs

import org.scalatest.funsuite.AnyFunSuite

final class RedoSourceSpec extends AnyFunSuite {

  private def supplier(items: String*): () => String = {
    val it = items.iterator
    () => if (it.hasNext) it.next() else null
  }

  test("drains until the queue signals empty (null)") {
    assert(RedoSource.drainBatch(supplier("a", "b", null), limit = 100) == Seq("a", "b"))
  }

  test("treats an empty string as end-of-queue") {
    assert(RedoSource.drainBatch(supplier("a", ""), limit = 100) == Seq("a"))
  }

  test("respects the batch limit and does not over-pull") {
    var calls = 0
    val get = () => { calls += 1; "x" } // infinite supply
    val batch = RedoSource.drainBatch(get, limit = 3)
    assert(batch == Seq("x", "x", "x"))
    assert(calls == 3, "must not call getRedoRecord beyond the limit")
  }

  test("an immediately-empty queue yields nothing with a single probe") {
    var calls = 0
    val get = () => { calls += 1; null }
    assert(RedoSource.drainBatch(get, limit = 10).isEmpty)
    assert(calls == 1)
  }

  test("rejects a non-positive limit") {
    assertThrows[IllegalArgumentException](RedoSource.drainBatch(() => "x", limit = 0))
  }
}
