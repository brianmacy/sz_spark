package com.senzing.spark.nativelib

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite

final class EngineSettingsSpec extends AnyFunSuite {

  private val mapper = new ObjectMapper()

  test("fromEnv returns the value when set") {
    assert(
      EngineSettings.fromEnv(Map(EngineSettings.EnvVar -> """{"SQL":{}}""")) == """{"SQL":{}}"""
    )
  }

  test("fromEnv fails loudly when unset or blank") {
    assertThrows[IllegalStateException](EngineSettings.fromEnv(Map.empty))
    assertThrows[IllegalStateException](EngineSettings.fromEnv(Map(EngineSettings.EnvVar -> "  ")))
  }

  test("rewritePaths sets the three PIPELINE keys to distinct extracted dirs") {
    val paths = NativePaths.under(new File("/var/tmp/sz-spark-abc123"))
    val json =
      """{"PIPELINE":{"SUPPORTPATH":"/x","RESOURCEPATH":"/y","CONFIGPATH":"/z"},"SQL":{"CONNECTION":"postgresql://h/db"}}"""
    val out = mapper.readTree(EngineSettings.rewritePaths(json, paths))
    val p = out.get("PIPELINE")
    assert(p.get("SUPPORTPATH").asText == paths.dataDir.getAbsolutePath)
    assert(p.get("RESOURCEPATH").asText == paths.resourcesDir.getAbsolutePath)
    assert(p.get("CONFIGPATH").asText == paths.configDir.getAbsolutePath)
    // the three are distinct, and the SQL section is preserved untouched
    assert(
      Set(
        p.get("SUPPORTPATH").asText,
        p.get("RESOURCEPATH").asText,
        p.get("CONFIGPATH").asText
      ).size == 3
    )
    assert(out.get("SQL").get("CONNECTION").asText == "postgresql://h/db")
  }

  test("rewritePaths creates PIPELINE when absent") {
    val paths = NativePaths.under(new File("/var/tmp/sz-spark-xyz"))
    val out = mapper.readTree(EngineSettings.rewritePaths("""{"SQL":{}}""", paths))
    assert(out.get("PIPELINE").get("CONFIGPATH").asText == paths.configDir.getAbsolutePath)
  }

  test("pgSslMode defaults to require") {
    assert(EngineSettings.pgSslMode(Map.empty) == "require")
    assert(EngineSettings.pgSslMode(Map("PGSSLMODE" -> "verify-full")) == "verify-full")
  }
}
