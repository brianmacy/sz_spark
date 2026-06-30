package com.senzing.spark.work

import scala.collection.mutable.ListBuffer

import com.senzing.sdk._
import com.senzing.spark.model.{Op, StagingKind, StagingRow}
import org.scalatest.funsuite.AnyFunSuite

final class RecordWorkerSpec extends AnyFunSuite {

  private val rec = InputRecord("CUSTOMERS", "1001", """{"NAME_FULL":"Jane"}""")

  private def counters() = new Counters
  private def silentProgress(c: Counters = new Counters) =
    new ProgressLogger("p", everyN = 0, () => 0L, _ => ())

  private def worker(
      op: WorkerOp,
      verb: InputRecord => String,
      c: Counters = new Counters,
      breaker: CircuitBreaker = new CircuitBreaker(1000),
      clock: () => Long = () => 0L,
      backoff: Backoff = Backoff(baseMs = 1, maxMs = 1, budgetMs = 60000L),
      notFoundIsBenign: Boolean = false,
      forceConfigDrift: () => Boolean = () => false,
      progress: ProgressLogger = null,
      longRecordMs: Long = 300000L,
      longLog: String => Unit = _ => ()
  ): RecordWorker =
    new RecordWorker(
      op = op,
      runId = "run-1",
      verb = verb,
      counters = c,
      progress = if (progress != null) progress else new ProgressLogger("p", 0, () => 0L, _ => ()),
      backoff = backoff,
      breaker = breaker,
      longRecordMs = longRecordMs,
      clock = clock,
      sleep = _ => (),
      rnd = () => 0.5,
      forceConfigDrift = forceConfigDrift,
      notFoundIsBenign = notFoundIsBenign,
      longRecordLog = longLog
    )

  test("add success emits one affected row per entity id") {
    val c = counters()
    val w =
      worker(WorkerOp.Add, _ => """{"AFFECTED_ENTITIES":[{"ENTITY_ID":10},{"ENTITY_ID":11}]}""", c)
    val rows = w.processOne(rec)
    assert(rows.size == 2)
    assert(rows.forall(_.kind == StagingKind.Affected))
    assert(rows.map(r => StagingRow.toAffected(r).entityId).toSet == Set(10L, 11L))
    assert(rows.head.op.contains(Op.Add))
    assert(c.succeeded == 1)
  }

  test("affected entity ids are deduped per task across records") {
    val c = counters()
    val w = worker(WorkerOp.Add, _ => """{"AFFECTED_ENTITIES":[{"ENTITY_ID":10}]}""", c)
    assert(w.processOne(rec).size == 1)
    assert(w.processOne(rec.copy(recordId = "1002")).isEmpty) // entity 10 already seen
    assert(c.succeeded == 2)
  }

  test("search emits one result row pairing request with result") {
    val w = worker(WorkerOp.Search, _ => """{"RESOLVED_ENTITIES":[]}""")
    val rows = w.processOne(rec)
    assert(rows.size == 1 && rows.head.kind == StagingKind.Search)
    val sr = StagingRow.toSearch(rows.head)
    assert(sr.requestJson == rec.payload && sr.resultJson == """{"RESOLVED_ENTITIES":[]}""")
  }

  test("bad input routes to an error row and continues (never throws)") {
    val c = counters()
    val w = worker(WorkerOp.Add, _ => throw new SzBadInputException("bad"), c)
    val rows = w.processOne(rec)
    assert(rows.size == 1 && rows.head.kind == StagingKind.Error)
    assert(StagingRow.toError(rows.head).category == "BAD_INPUT")
    assert(c.errored == 1)
  }

  test("systemic error fails the task (rethrown)") {
    val w = worker(WorkerOp.Add, _ => throw new SzLicenseException("no license"))
    assertThrows[SzLicenseException](w.processOne(rec))
  }

  test("config-relevant error reinits and retries once on success") {
    val c = counters()
    var calls = 0
    val verb: InputRecord => String = { _ =>
      calls += 1
      if (calls == 1) throw new SzConfigurationException("stale config")
      else """{"AFFECTED_ENTITIES":[{"ENTITY_ID":7}]}"""
    }
    val w = worker(WorkerOp.Add, verb, c, forceConfigDrift = () => true)
    val rows = w.processOne(rec)
    assert(calls == 2)
    assert(rows.size == 1 && c.succeeded == 1)
  }

  test("config-relevant error with no config change routes to error") {
    val c = counters()
    val w = worker(
      WorkerOp.Add,
      _ => throw new SzConfigurationException("x"),
      c,
      forceConfigDrift = () => false
    )
    val rows = w.processOne(rec)
    assert(rows.head.kind == StagingKind.Error)
    assert(StagingRow.toError(rows.head).category == "CONFIG_RELEVANT")
  }

  test("NOT_FOUND is a benign skip only when configured") {
    val cBenign = counters()
    val benign = worker(
      WorkerOp.Delete,
      _ => throw new SzNotFoundException("absent"),
      cBenign,
      notFoundIsBenign = true
    )
    assert(benign.processOne(rec).isEmpty && cBenign.notFound == 1)

    val cErr = counters()
    val strict = worker(
      WorkerOp.Delete,
      _ => throw new SzNotFoundException("absent"),
      cErr,
      notFoundIsBenign = false
    )
    assert(
      StagingRow.toError(strict.processOne(rec).head).category == "BAD_INPUT" && cErr.errored == 1
    )
  }

  test("RETRY_EXHAUSTED from the SDK does not re-enter backoff") {
    val c = counters()
    var calls = 0
    val w = worker(
      WorkerOp.Add,
      { _ => calls += 1; throw new SzRetryTimeoutExceededException("done") },
      c
    )
    val rows = w.processOne(rec)
    assert(calls == 1, "must not retry an already-exhausted error")
    assert(StagingRow.toError(rows.head).category == "RETRY_EXHAUSTED")
    assert(c.retried == 0)
  }

  test("retryable error is retried then succeeds") {
    val c = counters()
    var calls = 0
    val verb: InputRecord => String = { _ =>
      calls += 1
      if (calls <= 2) throw new SzDatabaseConnectionLostException("blip")
      else """{"AFFECTED_ENTITIES":[{"ENTITY_ID":1}]}"""
    }
    val w = worker(WorkerOp.Add, verb, c)
    val rows = w.processOne(rec)
    assert(calls == 3 && c.retried == 2 && c.succeeded == 1 && rows.size == 1)
  }

  test("retryable budget exhaustion routes to RETRY_EXHAUSTED") {
    val c = counters()
    // clock: deadline calc reads 0 (deadline=10), then each loop read jumps past it
    val ticks = Iterator(0L, 5L, 100L, 100L, 100L) ++ Iterator.continually(100L)
    val w = worker(
      WorkerOp.Add,
      _ => throw new SzDatabaseConnectionLostException("down"),
      c,
      clock = () => ticks.next(),
      backoff = Backoff(baseMs = 1, maxMs = 1, budgetMs = 10L)
    )
    val rows = w.processOne(rec)
    assert(StagingRow.toError(rows.head).category == "RETRY_EXHAUSTED")
  }

  test("a tripped circuit breaker fails the partition fast") {
    val c = counters()
    val cb = new CircuitBreaker(threshold = 2)
    val w = worker(
      WorkerOp.Add,
      _ => throw new SzDatabaseConnectionLostException("down"),
      c,
      breaker = cb
    )
    assertThrows[SzDatabaseConnectionLostException](w.processOne(rec))
    assert(cb.tripped)
  }

  test("LONG_RECORD is logged and counted but never aborts") {
    val c = counters()
    val logs = ListBuffer.empty[String]
    val ticks = Iterator(0L, 0L, 400000L) ++ Iterator.continually(400000L)
    val w = worker(
      WorkerOp.Add,
      _ => """{"AFFECTED_ENTITIES":[{"ENTITY_ID":1}]}""",
      c,
      clock = () => ticks.next(),
      longRecordMs = 300000L,
      longLog = logs += _
    )
    val rows = w.processOne(rec)
    assert(rows.size == 1 && c.succeeded == 1 && c.long == 1)
    assert(logs.exists(_.contains("LONG_RECORD")))
  }

  test("progress is emitted once per record") {
    val emitted = ListBuffer.empty[String]
    val prog = new ProgressLogger("CUSTOMERS/part-0", everyN = 1, () => 0L, emitted += _)
    val w = worker(WorkerOp.Add, _ => """{"AFFECTED_ENTITIES":[]}""", progress = prog)
    w.processOne(rec); w.processOne(rec.copy(recordId = "2"))
    assert(emitted.size == 2)
  }
}
