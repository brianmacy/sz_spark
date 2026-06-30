package com.senzing.spark.work

import org.scalatest.funsuite.AnyFunSuite

final class BackoffSpec extends AnyFunSuite {

  private val b = Backoff(baseMs = 1000L, maxMs = 30000L, budgetMs = 60000L)

  test("delay is jittered within [exp/2, exp] and deterministic given rnd") {
    // attempt 0 → exp=1000 → range [500, 1000]
    assert(b.delayMs(0, 0.0) == 500L)
    assert(b.delayMs(0, 1.0) == 1000L)
    assert(b.delayMs(0, 0.5) == 750L)
  }

  test("delay grows exponentially and caps at maxMs") {
    assert(b.delayMs(1, 0.0) == 1000L) // exp=2000 → half=1000
    assert(b.delayMs(2, 0.0) == 2000L) // exp=4000
    assert(b.delayMs(100, 1.0) == 30000L) // capped at maxMs
  }

  test("budget boundary") {
    assert(b.withinBudget(59999L))
    assert(!b.withinBudget(60000L))
  }
}
