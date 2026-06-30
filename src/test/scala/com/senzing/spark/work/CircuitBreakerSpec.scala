package com.senzing.spark.work

import org.scalatest.funsuite.AnyFunSuite

final class CircuitBreakerSpec extends AnyFunSuite {

  test("trips after threshold consecutive failures") {
    val cb = new CircuitBreaker(threshold = 3)
    assert(!cb.tripped)
    cb.recordFailure(); cb.recordFailure()
    assert(!cb.tripped)
    cb.recordFailure()
    assert(cb.tripped)
    assert(cb.consecutiveFailures == 3)
  }

  test("a success resets the consecutive counter") {
    val cb = new CircuitBreaker(threshold = 2)
    cb.recordFailure()
    cb.recordSuccess()
    cb.recordFailure()
    assert(!cb.tripped)
    cb.recordFailure()
    assert(cb.tripped)
  }
}
