package com.senzing.spark.engine

import org.scalatest.funsuite.AnyFunSuite

final class SzEnvGuardSpec extends AnyFunSuite {

  test("a second build in the same JVM trips the guard; destroy frees the slot") {
    SzEnvGuard.resetForTest()
    SzEnvGuard.markBuilt()
    assert(SzEnvGuard.isActive)
    assertThrows[IllegalStateException](SzEnvGuard.markBuilt())
    SzEnvGuard.markDestroyed()
    assert(!SzEnvGuard.isActive)
    SzEnvGuard.markBuilt() // slot free again
    SzEnvGuard.resetForTest()
  }

  test("guardedBuild holds the slot on success and still refuses a second build") {
    SzEnvGuard.resetForTest()
    assert(SzEnvGuard.guardedBuild("env") == "env")
    assert(SzEnvGuard.isActive)
    assertThrows[IllegalStateException](SzEnvGuard.guardedBuild(fail("must not build")))
    SzEnvGuard.resetForTest()
  }

  test("a failed build frees the slot so the retry surfaces the real error, not 'already active'") {
    SzEnvGuard.resetForTest()
    val boom = new RuntimeException("bad settings")
    val first = intercept[RuntimeException](SzEnvGuard.guardedBuild(throw boom))
    assert(first eq boom)
    assert(!SzEnvGuard.isActive)
    val retry = intercept[RuntimeException](SzEnvGuard.guardedBuild(throw boom))
    assert(retry eq boom)
    SzEnvGuard.resetForTest()
  }
}
