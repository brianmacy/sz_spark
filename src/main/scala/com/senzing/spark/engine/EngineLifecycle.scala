package com.senzing.spark.engine

import com.senzing.sdk.SzEnvironment

/**
 * The per-JVM engine lifecycle, factored out of [[SzEngineProvider]] so its concurrency/ordering
 * contract is unit-testable with injected `build`/`destroy`/`cleanup` functions.
 *
 * Contract (see docs/DESIGN.md §3): build EXACTLY ONCE per JVM; `acquire`/`release` maintain a
 * liveness counter only and NEVER destroy at zero (Databricks reuses executor JVMs across stages);
 * `shutdown` runs once and destroys BEFORE native cleanup (unordered teardown can SIGSEGV against
 * unlinked .so files).
 */
final class EngineLifecycle(
    build: () => SzEnvironment,
    destroy: SzEnvironment => Unit,
    cleanup: () => Unit
) {
  private val lock = new Object
  private var env: SzEnvironment = null
  private var built = false
  private var destroyed = false
  private var liveness = 0

  /** Acquire the shared env, building it once on the first call. */
  def acquire(): SzEnvironment = lock.synchronized {
    if (destroyed) throw new IllegalStateException("EngineLifecycle already shut down")
    if (!built) {
      env = build()
      built = true
    }
    liveness += 1
    env
  }

  /** Release a reference. Decrements the liveness counter only — never destroys. */
  def release(): Unit = lock.synchronized {
    if (liveness > 0) liveness -= 1
  }

  /** Idempotent shutdown: destroy the env (if built) THEN run native cleanup, in that order. */
  def shutdown(): Unit = lock.synchronized {
    if (built && !destroyed) {
      try if (env != null) destroy(env)
      finally {
        destroyed = true
        cleanup()
      }
    }
  }

  def livenessCount: Int = lock.synchronized(liveness)
  def isBuilt: Boolean = lock.synchronized(built)
  def isDestroyed: Boolean = lock.synchronized(destroyed)
}
