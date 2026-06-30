package com.senzing.spark.work

/**
 * Per-partition consecutive-failure breaker. Trips after `threshold` consecutive retryable failures
 * so a sustained DB outage fails the partition fast (Spark task retry handles it once) instead of
 * each record burning its full retry budget. Single-threaded use by one worker.
 */
final class CircuitBreaker(val threshold: Int) {
  private var consecutive = 0

  def recordSuccess(): Unit = consecutive = 0
  def recordFailure(): Unit = consecutive += 1
  def tripped: Boolean = consecutive >= threshold
  def consecutiveFailures: Int = consecutive
}
