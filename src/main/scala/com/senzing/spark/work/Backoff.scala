package com.senzing.spark.work

/**
 * Jittered exponential backoff with a total budget. Pure: `delayMs` takes the random draw so tests
 * are deterministic; the worker supplies `ThreadLocalRandom.nextDouble()` and a clock.
 */
final case class Backoff(baseMs: Long = 1000L, maxMs: Long = 30000L, budgetMs: Long = 60000L) {

  /**
   * Delay for a 0-based attempt, jittered into [exp/2, exp], capped at `maxMs`. `rnd` is in [0,1).
   */
  def delayMs(attempt: Int, rnd: Double): Long = {
    val shift = math.min(math.max(attempt, 0), 20)
    val exp = math.min(baseMs * (1L << shift), maxMs)
    val half = exp / 2
    half + (math.min(math.max(rnd, 0.0), 1.0) * half).toLong
  }

  def withinBudget(elapsedMs: Long): Boolean = elapsedMs < budgetMs
}
