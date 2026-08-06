package com.senzing.spark.diag

/**
 * One engine getStats() sample: the executor JVM that produced it, the wall-clock epoch-ms it was
 * read, and the raw `getStats()` JSON. `getStats()` is process-global and RESET-ON-READ, so `stats`
 * is the DELTA since the previous read on this JVM (the intended Rust stats-thread pattern).
 *
 * Serializable so it can ride `PluginContext.send` from the executor back to the driver for a
 * single central log.
 */
final case class StatsSample(executorId: String, tsEpochMs: Long, stats: String)
    extends Serializable {

  /** Single-line, greppable rendering for the driver (or executor-fallback) log. */
  def line: String = s"SZ_STATS executor=$executorId ts=$tsEpochMs stats=$stats"
}

/**
 * The single-caller-per-JVM engine-stats sampler loop, factored out of [[StatsPlugin]] so its
 * cadence, engine-readiness wait, and shutdown contract are unit-testable with an injected engine
 * probe / clock / sleep / sink — no real `SzEngine`, no Spark.
 *
 * `getStats()` is process-global and reset-on-read, so EXACTLY ONE thread per engine JVM may call
 * it (the sanctioned exception to the "never call getStats from tasks" rule documented in
 * [[com.senzing.spark.work.ProgressLogger]] / [[com.senzing.spark.work.Counters]]).
 *
 * Contract:
 *   - Wait (bounded backoff) for a live engine before sampling — the engine builds lazily on the
 *     first task, AFTER the plugin's `init`. If NO engine ever appears, idle harmlessly forever and
 *     NEVER emit (a non-Senzing executor must not be sampled).
 *   - Never throw out of [[runLoop]]: a diagnostics failure must not crash the executor.
 *   - [[stop]] (with a thread interrupt) ends the loop promptly.
 *
 * @param engineProbe
 *   `None` until the engine is live, then `Some(engine)` (never vanishes again)
 * @param readStats
 *   how to read `getStats()` (the plugin wraps it in the engine read lock)
 * @param sleepMs
 *   injectable, interruptible sleep (real: `Thread.sleep`)
 * @param readyPollMs
 *   bounded backoff while waiting for the engine to appear
 */
final class StatsSampler[E](
    executorId: String,
    intervalMs: Long,
    engineProbe: () => Option[E],
    readStats: E => String,
    clock: () => Long,
    sink: StatsSample => Unit,
    sleepMs: Long => Unit,
    log: String => Unit,
    readyPollMs: Long = 1000L
) {

  @volatile private var running: Boolean = true

  def stop(): Unit = running = false
  def isRunning: Boolean = running

  /** Block (bounded backoff) until the engine is live; `None` if stopped before one appears. */
  private def awaitEngine(): Option[E] = {
    var e: Option[E] = None
    while (running && e.isEmpty) {
      e =
        try engineProbe()
        catch { case _: Throwable => None }
      if (e.isEmpty && running) sleepMs(readyPollMs)
    }
    if (running) e else None
  }

  /** The daemon-thread body. Never throws — a stats failure must not crash the executor. */
  def runLoop(): Unit =
    try
      awaitEngine() match {
        case None => () // stopped/interrupted before an engine ever appeared — idle exit
        case Some(_) =>
          while (running) {
            sleepMs(intervalMs)
            if (running) sampleOnce()
          }
      }
    catch {
      case _: InterruptedException => () // shutdown interrupt — clean exit
      case t: Throwable =>
        log(s"SZ_STATS sampler exiting on ${t.getClass.getName}: ${t.getMessage}")
    }

  /** Re-probe (the engine can only appear, never vanish) then emit one sample; swallow failures. */
  private def sampleOnce(): Unit =
    engineProbe().foreach { e =>
      try sink(StatsSample(executorId, clock(), readStats(e)))
      catch {
        case _: InterruptedException => running = false
        case t: Throwable =>
          log(
            s"SZ_STATS getStats failed on executor=$executorId: " +
              s"${t.getClass.getName}: ${t.getMessage}"
          )
      }
    }
}
