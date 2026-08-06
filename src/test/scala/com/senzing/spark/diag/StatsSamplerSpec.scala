package com.senzing.spark.diag

import scala.collection.mutable.ListBuffer

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for the sampler LOOP, with no real `SzEngine` and no Spark: the engine probe, clock,
 * sleep, and sink are all injected, and `sleepMs` doubles as the loop driver (it stops the sampler
 * after a fixed number of ticks so `runLoop` returns deterministically on the calling thread).
 */
final class StatsSamplerSpec extends AnyFunSuite {

  test("StatsSample.line is a single greppable SZ_STATS line") {
    assert(
      StatsSample("7", 1234L, "{\"a\":1}").line == "SZ_STATS executor=7 ts=1234 stats={\"a\":1}"
    )
  }

  test("emits one sample per interval with executor id, timestamp, and stats delta") {
    val emits = ListBuffer.empty[StatsSample]
    val sleeps = ListBuffer.empty[Long]
    var sampler: StatsSampler[String] = null
    sampler = new StatsSampler[String](
      executorId = "7",
      intervalMs = 300000L,
      engineProbe = () => Some("ENGINE"),
      readStats = e => s"STATS($e)",
      clock = () => 1234L,
      sink = emits += _,
      sleepMs = ms => { sleeps += ms; if (sleeps.size >= 3) sampler.stop() },
      log = _ => ()
    )
    sampler.runLoop()

    // sleep#1 -> emit, sleep#2 -> emit, sleep#3 -> stop (no emit) => 2 samples, all at the interval.
    assert(emits.size == 2)
    assert(sleeps.forall(_ == 300000L))
    assert(
      emits.forall(s => s.executorId == "7" && s.tsEpochMs == 1234L && s.stats == "STATS(ENGINE)")
    )
  }

  test("waits for engine readiness with bounded backoff and NEVER emits when none appears") {
    val emits = ListBuffer.empty[StatsSample]
    val sleeps = ListBuffer.empty[Long]
    var sampler: StatsSampler[String] = null
    sampler = new StatsSampler[String](
      executorId = "x",
      intervalMs = 300000L,
      engineProbe = () => None, // engine never appears (e.g. a non-Senzing executor)
      readStats = _ => "NEVER",
      clock = () => 0L,
      sink = emits += _,
      sleepMs = ms => { sleeps += ms; if (sleeps.size >= 5) sampler.stop() },
      log = _ => (),
      readyPollMs = 1000L
    )

    sampler.runLoop() // must return (not hang, not throw)

    assert(emits.isEmpty) // never sampled
    assert(
      sleeps.nonEmpty && sleeps.forall(_ == 1000L)
    ) // only the readiness backoff, never the interval
  }

  test("a getStats failure does not crash the loop and emits nothing for that tick") {
    val emits = ListBuffer.empty[StatsSample]
    val logs = ListBuffer.empty[String]
    var sampler: StatsSampler[String] = null
    var calls = 0
    sampler = new StatsSampler[String](
      executorId = "9",
      intervalMs = 1000L,
      engineProbe = () => Some("E"),
      readStats = _ => { calls += 1; throw new RuntimeException("boom") },
      clock = () => 0L,
      sink = emits += _,
      sleepMs = _ => if (calls >= 1) sampler.stop(),
      log = logs += _
    )

    sampler.runLoop() // returns normally despite readStats throwing

    assert(emits.isEmpty)
    assert(logs.exists(_.contains("getStats failed")))
  }

  test("stop() ends the loop") {
    var sampler: StatsSampler[String] = null
    val emits = ListBuffer.empty[StatsSample]
    sampler = new StatsSampler[String](
      executorId = "1",
      intervalMs = 10L,
      engineProbe = () => Some("E"),
      readStats = _ => "S",
      clock = () => 0L,
      sink = emits += _,
      sleepMs = _ => sampler.stop(), // stop on the very first sleep, before any emit
      log = _ => ()
    )
    sampler.runLoop()
    assert(emits.isEmpty)
    assert(!sampler.isRunning)
  }
}
