package com.senzing.spark.engine

import java.util.concurrent.atomic.AtomicLong

import com.senzing.sdk.SzEnvironment

/**
 * Detects config drift and reinitializes the engine to the database's current default config — this
 * is what enables **live config updates**: a steward/admin activates a new default config in the DB
 * and running jobs adopt it without a restart.
 *
 * Compares `env.getActiveConfigId()` to `env.getConfigManager().getDefaultConfigId()` — BOTH on
 * `SzEnvironment` (not `SzEngine`) — and, when they differ, runs `env.reinitialize(default)` inside
 * the caller-supplied write-lock runner so in-flight verbs (which hold the read lock) quiesce
 * first. The reinit is **double-checked under the lock** so only ONE thread reinitializes —
 * concurrent detection (or an error storm on the force path) never stacks reinitializations. The
 * periodic path is additionally throttled (~`intervalMs`) via a CAS timestamp; the config-relevant
 * retry path bypasses the throttle but is still bounded to a single reinit by the in-lock recheck.
 */
final class ConfigDrift(
    intervalMs: Long = 60000L,
    clock: () => Long = () => System.currentTimeMillis()
) {
  private val lastCheckMs = new AtomicLong(0L)

  /** Throttled drift check (call between records). Returns true iff a reinit happened. */
  def maybeReinit(env: SzEnvironment, underWriteLock: (=> Unit) => Unit): Boolean = {
    val now = clock()
    val last = lastCheckMs.get()
    if (now - last < intervalMs) false
    else if (!lastCheckMs.compareAndSet(last, now)) false // another thread owns this check window
    else doCheck(env, underWriteLock)
  }

  /** Unthrottled drift check for the config-relevant error retry path. */
  def forceCheckAndReinit(env: SzEnvironment, underWriteLock: (=> Unit) => Unit): Boolean = {
    lastCheckMs.set(clock())
    doCheck(env, underWriteLock)
  }

  private def doCheck(env: SzEnvironment, underWriteLock: (=> Unit) => Unit): Boolean = {
    // Cheap unlocked check first — the common case is no drift, so we avoid taking the write lock
    // (which would block all in-flight verbs) unless a reinit is actually needed.
    if (env.getActiveConfigId == env.getConfigManager.getDefaultConfigId) false
    else {
      var reinitialized = false
      // Double-checked under the write lock: only the FIRST thread reinitializes. Threads that were
      // waiting on the lock re-read the ids, see active == default, and skip — so concurrent drift
      // detection (or an error storm down the force path) can never STACK reinitializations.
      underWriteLock {
        val default = env.getConfigManager.getDefaultConfigId
        if (env.getActiveConfigId != default) {
          env.reinitialize(default)
          reinitialized = true
        }
      }
      reinitialized
    }
  }
}
