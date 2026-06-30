package com.senzing.spark.engine

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ListBuffer

import com.senzing.sdk.SzEnvironment
import org.mockito.Mockito
import org.scalatest.funsuite.AnyFunSuite

/** Covers the per-JVM engine lifecycle contract used by SzEngineProvider. */
final class EngineLifecycleSpec extends AnyFunSuite {

  private def fakeEnv(): SzEnvironment = Mockito.mock(classOf[SzEnvironment])

  test("builds exactly once under concurrent acquire; same instance; liveness counted") {
    val builds = new AtomicInteger(0)
    val env = fakeEnv()
    val life = new EngineLifecycle(() => { builds.incrementAndGet(); env }, _ => (), () => ())
    val got = new java.util.concurrent.ConcurrentLinkedQueue[SzEnvironment]()
    val ts = (1 to 8).map(_ => new Thread(() => got.add(life.acquire())))
    ts.foreach(_.start()); ts.foreach(_.join())
    assert(builds.get() == 1)
    assert(life.livenessCount == 8)
    assert(got.toArray.forall(_ eq env))
  }

  test("release to zero never destroys, and a later acquire does not rebuild") {
    val builds = new AtomicInteger(0)
    val destroys = new AtomicInteger(0)
    val life = new EngineLifecycle(
      () => { builds.incrementAndGet(); fakeEnv() },
      _ => destroys.incrementAndGet(),
      () => ()
    )
    life.acquire(); life.acquire(); life.release(); life.release()
    assert(life.livenessCount == 0)
    assert(destroys.get() == 0, "release-to-zero must NOT destroy")
    assert(!life.isDestroyed)
    life.acquire() // reused JVM, new stage
    assert(builds.get() == 1, "must not rebuild after release-to-zero")
  }

  test("shutdown destroys exactly once, BEFORE native cleanup, and is idempotent") {
    val order = ListBuffer.empty[String]
    val life =
      new EngineLifecycle(() => fakeEnv(), _ => order += "destroy", () => order += "cleanup")
    life.acquire()
    life.shutdown()
    life.shutdown() // idempotent
    assert(order.toList == List("destroy", "cleanup"))
  }

  test("acquire after shutdown fails loudly") {
    val life = new EngineLifecycle(() => fakeEnv(), _ => (), () => ())
    life.acquire(); life.shutdown()
    assertThrows[IllegalStateException](life.acquire())
  }
}
