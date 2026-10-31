package com.senzing.spark.engine

import com.senzing.spark.nativelib.EngineSettings
import org.scalatest.funsuite.AnyFunSuite

final class EngineConfigPinSpec extends AnyFunSuite {

  private val env = Map(EngineSettings.EnvVar -> """{"SQL":{"CONNECTION":"env"}}""")
  private val explicit = """{"SQL":{"CONNECTION":"explicit"}}"""

  test("an explicit config wins over the environment") {
    val pin = new EngineConfigPin
    pin.offer(Some(explicit))
    assert(pin.resolve(env) == explicit)
  }

  test("no explicit config (None or blank) falls back to the environment") {
    val pin = new EngineConfigPin
    pin.offer(None)
    pin.offer(Some("  "))
    assert(pin.resolve(env) == env(EngineSettings.EnvVar))
  }

  test("an explicit config alone is enough (environment unset)") {
    val pin = new EngineConfigPin
    pin.offer(Some(explicit))
    assert(pin.resolve(Map.empty) == explicit)
  }

  test("the same explicit config may be offered again; a different one fails loudly") {
    val pin = new EngineConfigPin
    pin.offer(Some(explicit))
    pin.offer(Some(s"  $explicit  ")) // trimmed: same value
    pin.offer(None)
    assertThrows[IllegalStateException](pin.offer(Some("""{"SQL":{"CONNECTION":"other"}}""")))
  }

  test("once built from the environment, a different explicit config fails loudly") {
    val pin = new EngineConfigPin
    assert(pin.resolve(env) == env(EngineSettings.EnvVar))
    pin.offer(Some(env(EngineSettings.EnvVar)))
    assertThrows[IllegalStateException](pin.offer(Some(explicit)))
  }

  test("neither explicit nor environment fails loudly") {
    assertThrows[IllegalStateException](new EngineConfigPin().resolve(Map.empty))
  }
}
