package com.senzing.spark.diag

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.api.plugin.PluginContext
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite

/**
 * Plugin-level tests that stay engine-free: the `PluginContext` is a Mockito mock and no engine is
 * ever built (`SzEngineProvider.tryEngine` returns `None` in this JVM), so the sampler thread parks
 * harmlessly in its readiness wait. This exercises the real thread wiring — start exactly one named
 * daemon, and stop it on `shutdown` — without a live Senzing engine or a Spark cluster.
 */
final class StatsPluginSpec extends AnyFunSuite {

  private def samplerThreads(): Seq[Thread] =
    Thread.getAllStackTraces.keySet.asScala.toSeq.filter(t =>
      t.isAlive && t.getName == StatsPlugin.ThreadName
    )

  /** Busy-wait up to `timeoutMs` for `p` to hold. */
  private def eventually(timeoutMs: Long)(p: => Boolean): Boolean = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (System.currentTimeMillis() < deadline && !p) Thread.sleep(20)
    p
  }

  test("executor plugin starts exactly one daemon sampler thread and shutdown stops it") {
    val ctx = mock(classOf[PluginContext])
    when(ctx.executorID()).thenReturn("3")
    when(ctx.conf()).thenReturn(new SparkConf(false).set(StatsPlugin.IntervalConfKey, "300000"))

    val exec = (new StatsPlugin).executorPlugin()
    exec.init(ctx, java.util.Collections.emptyMap())

    try {
      assert(
        eventually(2000)(samplerThreads().size == 1),
        "exactly one sampler thread should start"
      )
      val t = samplerThreads().head
      assert(t.isDaemon, "sampler thread must be a daemon")
    } finally exec.shutdown()

    assert(eventually(3000)(samplerThreads().isEmpty), "shutdown must stop the sampler thread")
  }

  test("driver plugin receive logs samples and unexpected messages without throwing") {
    val d = (new StatsPlugin).driverPlugin()
    assert(d.receive(StatsSample("1", 5L, "{}")) == null)
    assert(d.receive("garbage") == null)
  }
}
