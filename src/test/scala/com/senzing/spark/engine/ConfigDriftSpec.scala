package com.senzing.spark.engine

import com.senzing.sdk.{SzConfigManager, SzEnvironment}
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite

final class ConfigDriftSpec extends AnyFunSuite {

  private def envWith(active: Long, default: Long): SzEnvironment = {
    val env = mock(classOf[SzEnvironment])
    val cm = mock(classOf[SzConfigManager])
    when(env.getActiveConfigId).thenReturn(active)
    when(env.getConfigManager).thenReturn(cm)
    when(cm.getDefaultConfigId).thenReturn(default)
    env
  }

  test("reinitializes under the write lock when active != default") {
    val env = envWith(active = 1L, default = 2L)
    var lockUsed = false
    val cd = new ConfigDrift(intervalMs = 60000L, clock = () => 100000L)
    val did = cd.maybeReinit(env, body => { lockUsed = true; body })
    assert(did)
    assert(lockUsed, "reinit must run inside the write-lock runner")
    verify(env).reinitialize(2L)
  }

  test("no reinit when active == default") {
    val env = envWith(active = 5L, default = 5L)
    val cd = new ConfigDrift(intervalMs = 0L, clock = () => 1L)
    assert(!cd.maybeReinit(env, b => b))
    verify(env, never()).reinitialize(anyLong())
  }

  test("throttle suppresses a second check inside the interval") {
    val env = envWith(active = 1L, default = 2L)
    var now = 100000L
    val cd = new ConfigDrift(intervalMs = 60000L, clock = () => now)
    assert(cd.maybeReinit(env, b => b)) // first check at t=100000
    now = 100001L // 1ms later — within the 60s window
    assert(!cd.maybeReinit(env, b => b))
    verify(env, times(1)).reinitialize(2L)
  }

  test("forceCheckAndReinit bypasses the throttle") {
    val env = envWith(active = 1L, default = 2L)
    val cd = new ConfigDrift(intervalMs = Long.MaxValue, clock = () => 1L)
    assert(cd.forceCheckAndReinit(env, b => b))
    assert(cd.forceCheckAndReinit(env, b => b))
    verify(env, times(2)).reinitialize(2L)
  }

  test("concurrent drift detection reinitializes exactly once (no stacking)") {
    val active = new java.util.concurrent.atomic.AtomicLong(1L)
    val cm = mock(classOf[SzConfigManager])
    when(cm.getDefaultConfigId).thenReturn(2L)
    val env = mock(classOf[SzEnvironment])
    when(env.getConfigManager).thenReturn(cm)
    when(env.getActiveConfigId).thenAnswer(_ => active.get())
    // reinitialize makes active match default — a second reinit would be a stacking bug
    doAnswer { _ => active.set(2L); null }.when(env).reinitialize(anyLong())

    val rwl = new java.util.concurrent.locks.ReentrantReadWriteLock()
    val underWrite: (=> Unit) => Unit = body => {
      rwl.writeLock().lock();
      try body
      finally rwl.writeLock().unlock()
    }
    val cd = new ConfigDrift(intervalMs = Long.MaxValue, clock = () => 1L)
    val threads = (1 to 8).map(_ => new Thread(() => cd.forceCheckAndReinit(env, underWrite)))
    threads.foreach(_.start()); threads.foreach(_.join())

    verify(env, times(1)).reinitialize(2L) // only the FIRST thread reinitializes
  }
}
