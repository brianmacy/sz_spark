package com.senzing.spark.core

import com.senzing.sdk.SzEngine
import com.senzing.spark.engine.{ConfigDrift, SzEngineProvider}
import com.senzing.spark.work._

/**
 * The per-partition worker factory: binds the shared per-JVM engine + config drift to an
 * op-specific verb, with each verb under the read lock (a concurrent config reinit takes the write
 * lock). The factory runs on executors. Moved out of `jobs.RecordJob` so the engine-facing core
 * carries no transport/JSONL glue.
 */
object EngineWorker {

  private val ProgressEveryN = 10000L

  def factory(
      op: WorkerOp,
      runId: String,
      verb: SzEngine => InputRecord => String
  ): () => RecordWorker =
    () => {
      val env = SzEngineProvider.acquire()
      val engine = env.getEngine()
      val drift = new ConfigDrift()
      val base = verb(engine)
      new RecordWorker(
        op = op,
        runId = runId,
        verb = r => SzEngineProvider.withReadLock(base(r)),
        counters = new Counters,
        progress = new ProgressLogger(
          op.tag,
          ProgressEveryN,
          () => System.currentTimeMillis(),
          msg => System.out.println(msg)
        ),
        maybeConfigDrift = () => drift.maybeReinit(env, b => SzEngineProvider.withWriteLock(b)),
        forceConfigDrift = () =>
          drift.forceCheckAndReinit(env, b => SzEngineProvider.withWriteLock(b))
      )
    }
}
