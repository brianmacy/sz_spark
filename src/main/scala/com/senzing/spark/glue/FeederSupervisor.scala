package com.senzing.spark.glue

import scala.util.control.NonFatal

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import com.senzing.spark.work.Backoff

/**
 * Driver-side SUPERVISION for the long-running feeder: keep a standalone-cluster feeder consuming
 * across executor/worker loss WITHOUT a manual restart.
 *
 * WHY THIS EXISTS: [[OverlappingBatchEngine]] already never DROPS a chunk (an uncommitted chunk is
 * re-read from the committed watermark on the next attempt). But a standalone-cluster executor loss
 * ("all executors lost" / repeated `SparkException`s / a silent scheduler hang) is not the same as
 * a single poison chunk — every in-flight job fails or blocks, the committed watermark freezes, and
 * the driver either spins failing every chunk or blocks forever. Observed 2026-08-09: a `.142`
 * worker restart left the feeder consuming nothing until a human restarted it. This supervisor
 * turns that into automatic recovery.
 *
 * MECHANISM — three cooperating pieces (see also [[OverlappingBatchEngine]]'s abort hooks):
 *   1. The engine, when told to, ABORTS its worker loop on a cluster-level signal (a burst of
 *      consecutive chunk failures, or a progress watchdog firing) by throwing
 *      [[FeederSupervisor.ClusterUnhealthyException]] out of `run` — instead of silently swallowing
 *      the failures and racing the cursor ahead of a frozen watermark.
 *   2. [[supervise]] catches that (and raw Spark executor-loss exceptions), logs loudly, waits for
 *      executors to re-register, backs off (bounded, jittered — reuses [[Backoff]]), and RESUBMITS
 *      a fresh engine attempt. A fresh attempt re-reads `RecordSource.initialCursor` = the
 *      committed offset, so it resumes exactly where the durable watermark left off.
 *   3. [[awaitCluster]] gates each resubmit on executor availability by polling
 *      `SparkContext.statusTracker.getExecutorInfos`.
 *
 * OFFSET SEMANTICS (AT-LEAST-ONCE, by design): recovery resumes from the last durably-committed
 * offset ([[OffsetWatermark]]), never from the beginning and never skipping a gap. The at-most a
 * few in-flight-but-uncommitted batches at the moment of failure are RE-READ on the retry — a
 * handful of already-resolved records, each a cheap optimized no-op re-add. `add_record` is
 * idempotent on re-add, so at-least-once is safe and correct here; exactly-once is neither offered
 * nor needed.
 */
object FeederSupervisor {

  /**
   * Raised by [[OverlappingBatchEngine]] to abort a run when the cluster (not one chunk) is
   * unhealthy, and by [[supervise]] when a bounded recovery budget is exhausted.
   */
  final class ClusterUnhealthyException(message: String, cause: Throwable)
      extends RuntimeException(message, cause) {
    def this(message: String) = this(message, null)
  }

  /**
   * @param minExecutors
   *   resubmit only once at least this many executors are registered.
   * @param maxAttempts
   *   total engine attempts before giving up; `0` = unbounded (never give up — the fleet
   *   philosophy: the work has to get done).
   * @param backoff
   *   bounded, jittered delay between attempts (its `maxMs` caps the delay).
   * @param executorWaitMs
   *   how long each gate polls for executors before resubmitting anyway.
   */
  final case class SupervisionConfig(
      minExecutors: Int = 1,
      maxAttempts: Int = 0,
      backoff: Backoff = Backoff(baseMs = 5000L, maxMs = 120000L, budgetMs = Long.MaxValue),
      executorWaitMs: Long = 300000L
  )

  // scalastyle:off println
  private def stdout(msg: String): Unit = println(s"[FeederSupervisor] $msg")
  private def stderr(msg: String): Unit = Console.err.println(s"[FeederSupervisor] ERROR: $msg")
  // scalastyle:on println

  /** Registered executors, excluding the always-present driver entry. */
  def executorCount(sc: SparkContext): Int =
    math.max(0, sc.statusTracker.getExecutorInfos.length - 1)

  /**
   * Is `t` a recover-by-resubmit failure (executor loss / cluster hang), as opposed to a fatal JVM
   * error we must not paper over? A [[ClusterUnhealthyException]] always is; otherwise walk the
   * cause chain for the Spark executor-loss signatures. Fatal errors (`VirtualMachineError`, etc.)
   * are never recoverable.
   */
  def isRecoverable(t: Throwable): Boolean = t match {
    case _: ClusterUnhealthyException => true
    case NonFatal(_) =>
      causeChain(t).exists { e =>
        val cls = e.getClass.getName
        val msg = Option(e.getMessage).getOrElse("")
        cls.contains("SparkException") ||
        msg.contains("ExecutorLostFailure") ||
        msg.contains("Lost executor") ||
        msg.contains("all executors") ||
        msg.contains("Executor heartbeat timed out")
      }
    case _ => false // fatal (VirtualMachineError, InterruptedException, LinkageError, ...)
  }

  private def causeChain(t: Throwable): List[Throwable] = {
    val buf = scala.collection.mutable.ListBuffer.empty[Throwable]
    var cur: Throwable = t
    while (cur != null && !buf.contains(cur)) { buf += cur; cur = cur.getCause }
    buf.toList
  }

  /**
   * Poll `countExecutors` until it reaches `minExecutors` or `maxWaitMs` elapses, sleeping a
   * bounded jittered backoff between polls. Returns whether the target was met. Pure/injectable
   * (clock, sleep, rnd, count) so the polling+backoff logic is unit-testable without a live
   * cluster.
   */
  def awaitCluster(
      minExecutors: Int,
      maxWaitMs: Long,
      countExecutors: () => Int,
      backoff: Backoff,
      sleep: Long => Unit,
      rnd: () => Double,
      now: () => Long,
      log: String => Unit
  ): Boolean = {
    val deadline = now() + maxWaitMs
    var attempt = 0
    var count = countExecutors()
    while (count < minExecutors && now() < deadline) {
      val d = backoff.delayMs(attempt, rnd())
      log(s"waiting for executors: have $count, need >=$minExecutors; re-checking in ${d}ms")
      sleep(d)
      attempt += 1
      count = countExecutors()
    }
    count >= minExecutors
  }

  /**
   * Run `attempt` and, on a recoverable failure, gate on executor availability, back off, and
   * re-run — until `attempt` returns normally (drained) or the recovery budget is exhausted. Pure:
   * every effect (attempt body, cluster gate, sleep, randomness, logging) is injected so the retry
   * / resume / backoff behavior is unit-testable with no Spark.
   *
   * `attempt(i)` receives the 0-based attempt index; on a fresh attempt it MUST re-read the
   * source's committed cursor (a fresh [[OverlappingBatchEngine.run]] does), which is what makes
   * recovery resume from the committed offset rather than the beginning.
   */
  def supervise[A](
      cfg: SupervisionConfig,
      attempt: Int => A,
      awaitCluster: () => Boolean,
      sleep: Long => Unit,
      rnd: () => Double,
      log: String => Unit,
      logErr: String => Unit,
      recoverable: Throwable => Boolean = isRecoverable
  ): A = {
    awaitCluster() // initial readiness gate before the first submit
    var i = 0
    var result: Option[A] = None
    while (result.isEmpty) {
      try result = Some(attempt(i))
      catch {
        case t if recoverable(t) =>
          i += 1
          logErr(
            s"feeder attempt aborted (recoverable): ${t.getClass.getSimpleName}: ${t.getMessage}"
          )
          if (cfg.maxAttempts > 0 && i >= cfg.maxAttempts)
            throw new ClusterUnhealthyException(
              s"recovery budget exhausted after $i attempts",
              t
            )
          val d = cfg.backoff.delayMs(i - 1, rnd())
          log(s"recovery: attempt #$i, backing off ${d}ms, then re-checking executors")
          sleep(d)
          if (!awaitCluster())
            logErr("executors did not re-register within the wait window; resubmitting anyway")
      }
    }
    result.get
  }

  /**
   * Live wiring: supervise a real [[OverlappingBatchEngine.run]] against a live `SparkContext`.
   * `engineAttempt` must (re)build nothing durable — it reuses the same [[RecordSource]], whose
   * committed watermark is re-read at the top of each engine run. Blocks for the life of the feeder
   * (`trigger=default`); returns the final [[OverlappingBatchEngine.Stats]] on a clean drain.
   */
  def run(
      spark: SparkSession,
      cfg: SupervisionConfig,
      engineAttempt: () => OverlappingBatchEngine.Stats
  ): OverlappingBatchEngine.Stats = {
    val sc = spark.sparkContext
    val gate: () => Boolean = () =>
      awaitCluster(
        cfg.minExecutors,
        cfg.executorWaitMs,
        () => executorCount(sc),
        cfg.backoff,
        ms => Thread.sleep(ms),
        () => java.util.concurrent.ThreadLocalRandom.current().nextDouble(),
        () => System.currentTimeMillis(),
        stdout
      )
    supervise[OverlappingBatchEngine.Stats](
      cfg,
      attempt = _ => engineAttempt(),
      awaitCluster = gate,
      sleep = ms => Thread.sleep(ms),
      rnd = () => java.util.concurrent.ThreadLocalRandom.current().nextDouble(),
      log = stdout,
      logErr = stderr
    )
  }
}
