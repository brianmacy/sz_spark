package com.senzing.spark.work

import scala.collection.mutable.ListBuffer

import org.scalatest.funsuite.AnyFunSuite

final class ProgressLoggerSpec extends AnyFunSuite {

  /** Deterministic clock advancing 1000ms per tick. */
  private class FakeClock(ticks: Seq[Long]) extends (() => Long) {
    private val it = ticks.iterator
    private var lastVal = ticks.head
    def apply(): Long = { if (it.hasNext) lastVal = it.next(); lastVal }
  }

  test("emits a labeled line every N records with interval and cumulative rates") {
    val out = ListBuffer.empty[String]
    // start=0, then a clock reading per record (everyN=2 → report at record 2)
    val clock = new FakeClock(Seq(0L, 1000L, 2000L))
    val log = new ProgressLogger("CUSTOMERS/part-3", everyN = 2, clock = clock, emit = out += _)
    val c = new Counters
    c.succeeded = 1; log.onRecord(c)
    c.succeeded = 2; log.onRecord(c) // triggers report at total=2
    assert(out.size == 1)
    val line = out.head
    assert(line.startsWith("[CUSTOMERS/part-3]"))
    assert(line.contains("total=2"))
    assert(line.contains("succeeded=2"))
    assert(line.contains("interval=") && line.contains("cumulative="))
  }

  test("does not emit before the cadence is reached") {
    val out = ListBuffer.empty[String]
    val log = new ProgressLogger("p", everyN = 100, () => 0L, out += _)
    val c = new Counters
    (1 to 50).foreach(_ => log.onRecord(c))
    assert(out.isEmpty)
  }

  test("final summary carries totals and elapsed") {
    val clock = new FakeClock(Seq(0L, 5000L))
    val log = new ProgressLogger("p", everyN = 0, clock, _ => ())
    val c = new Counters
    c.succeeded = 10
    log.onRecord(c)
    val s = log.finalSummary(c)
    assert(
      s.contains("DONE") && s.contains("total=1") && s.contains("elapsed=") && s.contains("avg=")
    )
  }
}
