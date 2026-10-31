package com.senzing.spark.engine

import java.util.concurrent.atomic.AtomicReference

import com.senzing.spark.nativelib.EngineSettings

/**
 * Which engine config this JVM's single environment is built from. An explicit value supplied by
 * the driver (carried to executors in the task closure) wins over
 * `SENZING_ENGINE_CONFIGURATION_JSON`. The first config seen is pinned: the JVM has exactly one
 * environment, so a later DIFFERENT explicit value fails loudly instead of being silently ignored.
 */
final class EngineConfigPin {
  private val pinned = new AtomicReference[String](null)

  /**
   * Record an explicit config (None or blank = no explicit value). Throws on a conflicting value.
   */
  def offer(explicit: Option[String]): Unit =
    EngineSettings.explicitValue(explicit).foreach { c =>
      if (!pinned.compareAndSet(null, c) && pinned.get != c)
        throw new IllegalStateException(
          "a different Senzing engine config is already in use in this JVM (one environment per JVM)"
        )
    }

  /**
   * The config to build from: the pinned explicit value, else the environment (which is then
   * pinned).
   */
  def resolve(env: Map[String, String]): String = {
    if (pinned.get == null) pinned.compareAndSet(null, EngineSettings.resolve(None, env))
    pinned.get
  }
}
