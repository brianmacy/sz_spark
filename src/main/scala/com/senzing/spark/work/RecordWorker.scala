package com.senzing.spark.work

import scala.annotation.tailrec

import com.senzing.spark.model._

/** One input record. For search, `recordId` may be empty and `payload` is the attributes JSON. */
final case class InputRecord(dataSource: String, recordId: String, payload: String)

/** Which verb this worker drives, and how its success output maps to output rows. */
sealed abstract class WorkerOp(val tag: String)
object WorkerOp {
  case object Add extends WorkerOp(Op.Add) // add/update
  case object Delete extends WorkerOp(Op.Delete)
  case object Redo extends WorkerOp(Op.Redo) // affected entities from processRedoRecord
  case object Search extends WorkerOp("SEARCH")
}

/**
 * Pure, engine-agnostic per-partition worker (single-threaded). The Senzing verb is injected as
 * `verb: InputRecord => String` (returns the WITH_INFO / search-result JSON, or throws an
 * `SzException`), so this whole loop is unit-testable without an engine or Spark.
 *
 * Per record it applies the error taxonomy (most-specific-first), retries `Retryable` with jittered
 * backoff under a budget + circuit breaker, cures config-relevant errors via a forced drift-reinit
 * + single retry, logs (never aborts) `LONG_RECORD`, dedups affected entity ids per task, and emits
 * tagged-union [[StagingRow]]s. A `Systemic` error (or a tripped breaker) is rethrown to fail the
 * task.
 */
final class RecordWorker(
    op: WorkerOp,
    runId: String,
    verb: InputRecord => String,
    counters: Counters,
    progress: ProgressLogger,
    backoff: Backoff = Backoff(),
    breaker: CircuitBreaker = new CircuitBreaker(threshold = 20),
    longRecordMs: Long = 300000L,
    clock: () => Long = () => System.currentTimeMillis(),
    sleep: Long => Unit = ms => Thread.sleep(ms),
    rnd: () => Double = () => java.util.concurrent.ThreadLocalRandom.current().nextDouble(),
    maybeConfigDrift: () => Boolean = () => false,
    forceConfigDrift: () => Boolean = () => false,
    notFoundIsBenign: Boolean = false,
    longRecordLog: String => Unit = _ => ()
) {
  import ErrorCategory._

  private val seenEntities = scala.collection.mutable.Set.empty[Long]

  /** Process one record, returning its output/error rows. Maintains per-task state. */
  def processOne(rec: InputRecord): Seq[StagingRow] = {
    maybeConfigDrift() // throttled, between records
    val rows = loop(rec, attempt = 0, configRetried = false, deadline = clock() + backoff.budgetMs)
    progress.onRecord(counters)
    rows
  }

  @tailrec
  private def loop(
      rec: InputRecord,
      attempt: Int,
      configRetried: Boolean,
      deadline: Long
  ): Seq[StagingRow] = {
    val start = clock()
    val result =
      try Right(verb(rec))
      catch { case t: Throwable => Left(t) }
    result match {
      case Right(out) =>
        recordLong(rec, start)
        breaker.recordSuccess()
        counters.succeeded += 1
        successRows(rec, out)

      case Left(t) =>
        ErrorTaxonomy.classify(t) match {
          case Systemic => throw t // fail the task loudly

          case NotFound if notFoundIsBenign =>
            counters.notFound += 1; Seq.empty // benign skip

          case NotFound =>
            counters.errored += 1; Seq(err(rec, t, BadInput, attempt))

          case BadInput =>
            counters.errored += 1; Seq(err(rec, t, BadInput, attempt))

          case ReplaceConflict =>
            counters.replaceConflict += 1
            if (!configRetried && forceConfigDrift())
              loop(rec, attempt, configRetried = true, deadline)
            else { counters.errored += 1; Seq(err(rec, t, ReplaceConflict, attempt)) }

          case ConfigRelevant =>
            if (!configRetried && forceConfigDrift())
              loop(rec, attempt, configRetried = true, deadline)
            else { counters.errored += 1; Seq(err(rec, t, ConfigRelevant, attempt)) }

          case RetryExhausted => // SDK already exhausted its internal retries — do NOT re-enter backoff
            counters.errored += 1; Seq(err(rec, t, RetryExhausted, attempt))

          case Retryable =>
            breaker.recordFailure()
            if (breaker.tripped)
              throw t // fail the partition fast; Spark task retry handles a sustained outage
            else if (clock() >= deadline) {
              counters.errored += 1; Seq(err(rec, t, RetryExhausted, attempt))
            } else {
              sleep(backoff.delayMs(attempt, rnd()))
              counters.retried += 1
              loop(rec, attempt + 1, configRetried, deadline)
            }
        }
    }
  }

  private def recordLong(rec: InputRecord, startMs: Long): Unit = {
    val elapsed = clock() - startMs
    if (elapsed > longRecordMs) {
      counters.long += 1
      longRecordLog(
        s"LONG_RECORD ${rec.dataSource}/${rec.recordId} took ${elapsed}ms (threshold ${longRecordMs}ms)"
      )
    }
  }

  private def successRows(rec: InputRecord, out: String): Seq[StagingRow] = op match {
    case WorkerOp.Search =>
      Seq(StagingRow.of(SearchResultRow(rec.payload, out)))
    case mutating =>
      InfoParser
        .affectedEntityIds(out)
        .filter(seenEntities.add) // dedup per task; Set.add returns true iff newly added
        .map(id =>
          StagingRow.of(AffectedEntityRow(rec.dataSource, rec.recordId, id, mutating.tag, runId))
        )
  }

  private def err(rec: InputRecord, t: Throwable, cat: ErrorCategory, attempts: Int): StagingRow =
    StagingRow.of(
      ErrorRow(
        rec.dataSource,
        rec.recordId,
        rec.payload,
        cat.name,
        ErrorTaxonomy.errorCode(t),
        Option(t.getMessage).getOrElse(t.getClass.getSimpleName),
        attempts
      )
    )
}
