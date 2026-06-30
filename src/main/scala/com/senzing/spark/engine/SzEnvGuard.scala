package com.senzing.spark.engine

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Process-wide guard enforcing Senzing's single-active-instance rule: only one `SzCoreEnvironment`
 * may be active per JVM. Both [[SzEngineProvider]] (executors) and `InitJob` route their build
 * through this so a second build in the same JVM fails loudly with a clear error rather than the
 * SDK's opaque ~6-minute corruption crash.
 */
object SzEnvGuard {
  private val active = new AtomicBoolean(false)

  /** Reserve the single slot; throw if one is already active in this JVM. */
  def markBuilt(): Unit =
    if (!active.compareAndSet(false, true))
      throw new IllegalStateException(
        "An SzEnvironment is already active in this JVM — only one instance may be active at a time"
      )

  /** Release the slot (called when the environment is destroyed). */
  def markDestroyed(): Unit = active.set(false)

  def isActive: Boolean = active.get()

  /** Test-only reset. */
  private[spark] def resetForTest(): Unit = active.set(false)
}
