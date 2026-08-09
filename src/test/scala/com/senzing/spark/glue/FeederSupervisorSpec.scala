package com.senzing.spark.glue

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite

import com.senzing.spark.glue.FeederSupervisor.{ClusterUnhealthyException, SupervisionConfig}
import com.senzing.spark.work.Backoff

/**
 * Supervision-loop unit tests — no Spark. Every effect ([[FeederSupervisor.supervise]]'s attempt
 * body, cluster gate, sleep, randomness) is injected, so the retry / RESUME-FROM-COMMITTED-OFFSET /
 * backoff / gate behavior is asserted deterministically. The resume assertions drive the REAL
 * [[OffsetWatermark]] on a local filesystem — the actual production resume path — proving recovery
 * neither re-reads from the beginning nor skips a gap.
 */
final class FeederSupervisorSpec extends AnyFunSuite {

  private val localFs: FileSystem = FileSystem.getLocal(new Configuration())

  private def newWatermark(start: Long): OffsetWatermark = {
    val dir = Files.createTempDirectory("wm").toFile.getAbsolutePath
    new OffsetWatermark(localFs, new Path(dir, "offset-t-0"), start)
  }

  private val fastBackoff = Backoff(baseMs = 100L, maxMs = 800L, budgetMs = Long.MaxValue)

  test("recovery resumes from the COMMITTED offset (not the beginning, no skip) and backs off") {
    // Real watermark = the production resume path. A "kafka lag" of 50 records, 10 per batch.
    val wm = newWatermark(start = 0L)
    val totalRecords = 50L
    val step = 10L

    val startsSeen = mutable.ArrayBuffer.empty[Long] // the cursor each attempt RESUMED from
    val attemptCalls = new AtomicInteger(0)

    // One engine attempt: read the committed cursor (what a fresh OverlappingBatchEngine.run does),
    // process [off, off+step) batches committing each. The FIRST attempt loses its executors after
    // committing exactly one batch → aborts with ClusterUnhealthyException (nothing beyond the
    // committed frontier was committed).
    def attempt(i: Int): Unit = {
      val resumeAt = wm.committedOffset
      startsSeen += resumeAt
      var off = resumeAt
      while (off < totalRecords) {
        val end = math.min(off + step, totalRecords)
        wm.complete(off, end) // commit advances the durable contiguous frontier
        off = end
        if (attemptCalls.getAndIncrement() == 0 && off == resumeAt + step)
          throw new ClusterUnhealthyException("all executors lost")
      }
    }

    val sleeps = mutable.ArrayBuffer.empty[Long]
    val gates = new AtomicInteger(0)

    FeederSupervisor.supervise[Unit](
      cfg = SupervisionConfig(maxAttempts = 5, backoff = fastBackoff),
      attempt = attempt,
      awaitCluster = () => { gates.incrementAndGet(); true },
      sleep = ms => { sleeps += ms; () },
      rnd = () => 0.0, // deterministic: attempt-0 delay = baseMs/2 = 50
      log = _ => (),
      logErr = _ => ()
    )

    assert(startsSeen.size == 2, s"exactly one retry (two attempts), was ${startsSeen.size}")
    assert(startsSeen(0) == 0L, s"first attempt cold-starts at 0, was ${startsSeen(0)}")
    assert(
      startsSeen(1) == step,
      s"RETRY RESUMES from the committed offset ($step), not 0, was ${startsSeen(1)}"
    )
    assert(wm.committedOffset == totalRecords, s"all committed once, was ${wm.committedOffset}")
    assert(sleeps == mutable.ArrayBuffer(50L), s"backed off once (jittered base/2), was $sleeps")
    // gate called once initially + once before the retry.
    assert(gates.get() == 2, s"executor gate checked before first submit and before retry, $gates")
  }

  test("a non-recoverable (fatal) error is rethrown WITHOUT retry") {
    val calls = new AtomicInteger(0)
    val err = new OutOfMemoryError("boom") // fatal — not an executor-loss failure
    val thrown = intercept[OutOfMemoryError] {
      FeederSupervisor.supervise[Unit](
        cfg = SupervisionConfig(backoff = fastBackoff),
        attempt = _ => { calls.incrementAndGet(); throw err },
        awaitCluster = () => true,
        sleep = _ => (),
        rnd = () => 0.0,
        log = _ => (),
        logErr = _ => ()
      )
    }
    assert(thrown eq err, "the fatal error propagated unchanged")
    assert(
      calls.get() == 1,
      s"attempted exactly once (no retry on a fatal error), was ${calls.get}"
    )
  }

  test("bounded maxAttempts gives up and the delay is capped at Backoff.maxMs") {
    val calls = new AtomicInteger(0)
    val sleeps = mutable.ArrayBuffer.empty[Long]
    intercept[ClusterUnhealthyException] {
      FeederSupervisor.supervise[Unit](
        cfg = SupervisionConfig(maxAttempts = 3, backoff = fastBackoff),
        attempt = _ => { calls.incrementAndGet(); throw new ClusterUnhealthyException("down") },
        awaitCluster = () => true,
        sleep = ms => { sleeps += ms; () },
        rnd = () => 1.0, // upper jitter bound
        log = _ => (),
        logErr = _ => ()
      )
    }
    assert(calls.get() == 3, s"exactly maxAttempts tries, was ${calls.get()}")
    // Two backoffs before the 2nd and 3rd tries: 100 (attempt idx 0), 200 (idx 1). None exceed maxMs.
    assert(sleeps == mutable.ArrayBuffer(100L, 200L), s"exponential, capped at maxMs, was $sleeps")
    assert(sleeps.forall(_ <= fastBackoff.maxMs), "every delay within the cap")
  }

  test("isRecoverable: executor-loss SparkException yes, plain data error no") {
    assert(FeederSupervisor.isRecoverable(new ClusterUnhealthyException("x")))
    assert(
      FeederSupervisor.isRecoverable(
        new RuntimeException(new RuntimeException("ExecutorLostFailure (executor 3 lost)"))
      ),
      "an ExecutorLostFailure anywhere in the cause chain is recoverable"
    )
    assert(!FeederSupervisor.isRecoverable(new IllegalArgumentException("bad record json")))
    assert(!FeederSupervisor.isRecoverable(new OutOfMemoryError("fatal")))
  }

  test("awaitCluster polls with backoff until minExecutors is reached, then returns true") {
    val counts = Iterator(0, 0, 2) // executors register on the 3rd poll
    val sleeps = mutable.ArrayBuffer.empty[Long]
    val ok = FeederSupervisor.awaitCluster(
      minExecutors = 1,
      maxWaitMs = 10_000L,
      countExecutors = () => counts.next(),
      backoff = fastBackoff,
      sleep = ms => { sleeps += ms; () },
      rnd = () => 0.0,
      now = () => 0L, // frozen clock ⇒ never hits the deadline; the count gate ends the loop
      log = _ => ()
    )
    assert(ok, "returned true once >= minExecutors executors were registered")
    assert(
      sleeps == mutable.ArrayBuffer(50L, 100L),
      s"two backoff polls (base/2 then base), $sleeps"
    )
  }

  test("awaitCluster returns false when executors never arrive within maxWaitMs") {
    val clock = new AtomicInteger(0)
    val ok = FeederSupervisor.awaitCluster(
      minExecutors = 1,
      maxWaitMs = 300L,
      countExecutors = () => 0, // never registers
      backoff = fastBackoff,
      sleep = _ => (),
      rnd = () => 0.0,
      now = () => clock.getAndAdd(100).toLong, // advances 100ms per read → crosses 300ms deadline
      log = _ => ()
    )
    assert(!ok, "gave up (returns false) once the wait window elapsed")
  }
}
