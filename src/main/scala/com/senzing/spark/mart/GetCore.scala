package com.senzing.spark.mart

import java.util.EnumSet

import org.apache.spark.TaskContext
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import com.senzing.sdk.{SzEngine, SzFlag, SzNotFoundException}
import com.senzing.spark.engine.SzEngineProvider
import com.senzing.spark.work.{ErrorCategory, ErrorTaxonomy}

/**
 * Tag for a [[GetResult]] row: found, absorbed/deleted (tombstone), or engine error (quarantine).
 */
object GetKind {
  final val Entity: String = "ENTITY"
  final val Gone: String = "GONE"
  final val Error: String = "ERROR"
}

/**
 * One `getEntity` outcome, tagged. Exactly one shape is populated per [[kind]]:
 *   - ENTITY: `json` is the full entity-read document; `category`/`errorCode`/`message` empty.
 *   - GONE: only `entityId`/`kind` — a [[SzNotFoundException]] ⇒ the entity was absorbed/deleted.
 *   - ERROR: `category` (an [[ErrorCategory]] name), `errorCode`, `message` — routed to
 *     `_quarantine`.
 */
final case class GetResult(
    entityId: Long,
    kind: String,
    json: String,
    category: String,
    errorCode: String,
    message: String
)

/**
 * The §7.4 replication flag set — an EXPLICIT OR of the specific flags the mart consumes, never a
 * `*_DEFAULT_FLAGS` composite (the Senzing production caution: DEFAULT membership can change across
 * versions and silently alter what is returned). Verified against the installed `sz-sdk.jar`:
 * `SZ_ENTITY_INCLUDE_ALL_RELATIONS` is itself a `Set<SzFlag>` composite (POSSIBLY_SAME |
 * POSSIBLY_RELATED | NAME_ONLY | DISCLOSED), so it is UNIONed with the singleton flags rather than
 * added as one enum value. Deliberately excluded: raw record `JSON_DATA`, candidate `*_KEY`
 * features, match-key details, feature stats (size/need trade — design O1).
 */
object ReplicationFlags {

  private val singles: java.util.Set[SzFlag] = EnumSet.of(
    SzFlag.SZ_ENTITY_INCLUDE_ENTITY_NAME,
    SzFlag.SZ_ENTITY_INCLUDE_RECORD_SUMMARY,
    SzFlag.SZ_ENTITY_INCLUDE_RECORD_DATA,
    SzFlag.SZ_ENTITY_INCLUDE_RECORD_MATCHING_INFO,
    SzFlag.SZ_ENTITY_INCLUDE_RECORD_DATES,
    SzFlag.SZ_ENTITY_INCLUDE_REPRESENTATIVE_FEATURES,
    SzFlag.SZ_ENTITY_INCLUDE_RELATED_MATCHING_INFO,
    SzFlag.SZ_ENTITY_INCLUDE_RELATED_RECORD_SUMMARY
  )

  /** The `Set<SzFlag>` passed to every replication `getEntity` call. */
  val Flags: java.util.Set[SzFlag] = SzFlag.union(singles, SzFlag.SZ_ENTITY_INCLUDE_ALL_RELATIONS)
}

/**
 * The engine bracket for the entity-mart refresh: a `getEntity` sweep over a partition of affected
 * entity ids. Mirrors [[com.senzing.spark.core.SparkRecordOps]] exactly — one `SzEnvironment` per
 * executor JVM (acquired at partition start, released on task completion via `TaskContext`, never a
 * premature `finally`), verbs under the read lock — and follows the same single-committed-staging
 * write / read-back discipline so the engine reads execute exactly once (no lineage re-execution
 * re-hitting the engine and DB).
 *
 * `getEntity` is a READ, not a mutating side effect, so this needs no retry/dedup machinery (that
 * is [[com.senzing.spark.work.RecordWorker]] for the add/delete/redo verbs); it classifies the one
 * failure that carries mart meaning — [[SzNotFoundException]] ⇒ GONE (tombstone) — routes any other
 * non-systemic engine error to ERROR (quarantine, message kept), and rethrows a `Systemic` error to
 * fail the task loudly (a Spark task retry handles a transient outage; the reads are idempotent).
 */
object GetCore {

  def run(
      spark: SparkSession,
      ids: Dataset[Long],
      stagingPath: String,
      flags: java.util.Set[SzFlag] = ReplicationFlags.Flags
  ): Dataset[GetResult] = {
    import spark.implicits._

    val staged: Dataset[GetResult] = ids.mapPartitions { it =>
      val env = SzEngineProvider.acquire()
      Option(TaskContext.get())
        .foreach(_.addTaskCompletionListener[Unit](_ => SzEngineProvider.release()))
      val engine = env.getEngine()
      it.map(id => getOne(engine, id, flags))
    }

    // One committed action — the only place the engine reads execute.
    staged.write.mode(SaveMode.Overwrite).parquet(stagingPath)

    // Read the committed table back (no engine re-execution downstream).
    spark.read.parquet(stagingPath).as[GetResult]
  }

  private def getOne(engine: SzEngine, id: Long, flags: java.util.Set[SzFlag]): GetResult =
    try {
      val json = SzEngineProvider.withReadLock(engine.getEntity(id, flags))
      GetResult(id, GetKind.Entity, json, "", "", "")
    } catch {
      case _: SzNotFoundException =>
        GetResult(id, GetKind.Gone, "", "", "", "")
      case t: Throwable =>
        ErrorTaxonomy.classify(t) match {
          case ErrorCategory.Systemic => throw t // fail the task loudly
          case cat => GetResult(id, GetKind.Error, "", cat.name, ErrorTaxonomy.errorCode(t), msg(t))
        }
    }

  private def msg(t: Throwable): String =
    Option(t.getMessage).getOrElse(t.getClass.getSimpleName)
}
