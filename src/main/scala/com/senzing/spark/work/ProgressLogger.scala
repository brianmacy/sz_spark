package com.senzing.spark.work

/**
 * Per-task progress logging: a labeled line every `everyN` records carrying the counter breakdown
 * plus BOTH the interval rate (since the last report — reveals slowdowns) and the cumulative
 * average rate. Clock + emit are injected for deterministic tests. Counts come only from in-memory
 * [[Counters]] — never `engine.getStats()` (which is process-global / reset-on-read).
 */
final class ProgressLogger(
    prefix: String,
    everyN: Long,
    clock: () => Long, // epoch millis
    emit: String => Unit
) {
  private val startMs = clock()
  private var total: Long = 0
  private var lastMs: Long = startMs
  private var lastCount: Long = 0

  /** Call once per processed record; emits a progress line every `everyN` records. */
  def onRecord(c: Counters): Unit = {
    total += 1
    if (everyN > 0 && total % everyN == 0) emit(line(c))
  }

  private def rate(count: Long, millis: Long): Double =
    if (millis <= 0) 0.0 else count * 1000.0 / millis

  private def line(c: Counters): String = {
    val now = clock()
    val interval = rate(total - lastCount, now - lastMs)
    val cum = rate(total, now - startMs)
    lastMs = now
    lastCount = total
    f"[$prefix] total=$total ${c.snapshot.brief} interval=$interval%.1f/s cumulative=$cum%.1f/s"
  }

  /** Final one-line summary at task end. */
  def finalSummary(c: Counters): String = {
    val elapsedMs = math.max(clock() - startMs, 0)
    val avg = rate(total, elapsedMs)
    f"[$prefix] DONE total=$total ${c.snapshot.brief} elapsed=${elapsedMs / 1000.0}%.1fs avg=$avg%.1f/s"
  }
}
