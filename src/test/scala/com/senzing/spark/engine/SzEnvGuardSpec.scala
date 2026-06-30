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
}
