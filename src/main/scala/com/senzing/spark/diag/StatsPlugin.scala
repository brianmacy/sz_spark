package com.senzing.spark.diag

import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Collections, Map => JMap}

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.slf4j.LoggerFactory

import com.senzing.sdk.SzEngine
import com.senzing.spark.engine.SzEngineProvider

/**
 * Sanctioned single-caller-per-JVM engine `getStats()` sampler, packaged as an
 * `org.apache.spark.api.plugin.SparkPlugin` so it is enabled purely by config (`--conf
 * spark.plugins=com.senzing.spark.diag.StatsPlugin`) — ZERO cost and zero code path when not
 * listed.
 *
 * Why a plugin (not a task-side call): `getStats()` is process-global and reset-on-read, so it must
 * be called by exactly ONE thread per engine JVM (see [[com.senzing.spark.work.ProgressLogger]] for
 * why tasks must never call it). `ExecutorPlugin.init` runs once per executor JVM, which is the one
 * place to start that single sampler thread.
 *
 * Design — driver-collect (preferred) over executor-log: each sample is shipped to the driver via
 * `PluginContext.send`, and [[StatsDriverPlugin.receive]] logs it, so all executors' samples land
 * in ONE driver log rather than N scattered executor stderrs. If `send` fails, the executor logs
 * the sample itself (same `SZ_STATS` line) so a sample is never silently dropped.
 */
object StatsPlugin {

  /** Interval between samples, ms. Default 5 min. */
  final val IntervalConfKey = "spark.senzing.statsIntervalMs"
  final val DefaultIntervalMs: Long = 300000L

  final val ThreadName = "sz-stats-sampler"

  /** Bounded backoff while waiting for the lazily-built engine to appear. */
  final val ReadyPollMs: Long = 1000L

  /** How long `shutdown` waits for the sampler thread to exit after interrupting it. */
  final val ShutdownJoinMs: Long = 2000L
}

final class StatsPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new StatsDriverPlugin
  override def executorPlugin(): ExecutorPlugin = new StatsExecutorPlugin
}

/** Driver side: logs each incoming stats sample centrally at INFO under the `SZ_STATS` prefix. */
final class StatsDriverPlugin extends DriverPlugin {
  private val log = LoggerFactory.getLogger(classOf[StatsDriverPlugin])

  override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
    log.info("SZ_STATS collector active (driver) — per-executor getStats samples log here")
    Collections.emptyMap()
  }

  /** Fire-and-forget receive for `PluginContext.send`; returns null. Never throws. */
  override def receive(message: Any): AnyRef = {
    message match {
      case s: StatsSample => log.info(s.line)
      case other => log.warn(s"SZ_STATS driver got unexpected message: ${String.valueOf(other)}")
    }
    null
  }
}

/**
 * Executor side: on the first (only) `init` per executor JVM, start ONE named daemon thread running
 * the [[StatsSampler]]. Guarded so a duplicate `init` is a no-op. Nothing here throws out of `init`
 * or the thread — diagnostics must never crash the executor.
 */
final class StatsExecutorPlugin extends ExecutorPlugin {
  private val log = LoggerFactory.getLogger(classOf[StatsExecutorPlugin])
  private val started = new AtomicBoolean(false)
  @volatile private var sampler: StatsSampler[SzEngine] = _
  @volatile private var thread: Thread = _

  override def init(ctx: PluginContext, extraConf: JMap[String, String]): Unit = {
    if (!started.compareAndSet(false, true)) return // exactly one sampler per JVM (defensive)
    try {
      val intervalMs = readInterval(ctx)
      val executorId = ctx.executorID()
      val s = new StatsSampler[SzEngine](
        executorId = executorId,
        intervalMs = intervalMs,
        engineProbe = () => SzEngineProvider.tryEngine(),
        // Read lock matches the verb path: concurrent with adds (also read-lock holders), serialized
        // only against a config-reinit write lock — so we do NOT serialize the whole engine.
        readStats = e => SzEngineProvider.withReadLock(e.getStats()),
        clock = () => System.currentTimeMillis(),
        sink = sample => emit(ctx, sample),
        sleepMs = ms => Thread.sleep(ms),
        log = msg => log.warn(msg),
        readyPollMs = StatsPlugin.ReadyPollMs
      )
      val t = new Thread(() => s.runLoop(), StatsPlugin.ThreadName)
      t.setDaemon(true)
      sampler = s
      thread = t
      t.start()
      log.info(s"SZ_STATS sampler started (executor=$executorId intervalMs=$intervalMs)")
    } catch {
      case t: Throwable =>
        log.warn(s"SZ_STATS sampler failed to start: ${t.getClass.getName}: ${t.getMessage}")
    }
  }

  /** Positive `spark.senzing.statsIntervalMs`, else the default. Never throws. */
  private def readInterval(ctx: PluginContext): Long =
    try {
      val v = ctx
        .conf()
        .get(StatsPlugin.IntervalConfKey, StatsPlugin.DefaultIntervalMs.toString)
        .trim
        .toLong
      if (v > 0) v else StatsPlugin.DefaultIntervalMs
    } catch { case _: Throwable => StatsPlugin.DefaultIntervalMs }

  /** Prefer central driver collection; fall back to the executor's own log so nothing is lost. */
  private def emit(ctx: PluginContext, sample: StatsSample): Unit =
    try ctx.send(sample)
    catch { case _: Throwable => log.info(sample.line) }

  override def shutdown(): Unit = {
    val s = sampler
    val t = thread
    if (s != null) s.stop()
    if (t != null) {
      t.interrupt()
      try t.join(StatsPlugin.ShutdownJoinMs)
      catch { case _: InterruptedException => Thread.currentThread().interrupt() }
    }
  }
}
