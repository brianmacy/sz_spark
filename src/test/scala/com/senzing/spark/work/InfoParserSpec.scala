package com.senzing.spark.work

import org.scalatest.funsuite.AnyFunSuite

final class InfoParserSpec extends AnyFunSuite {

  test("parses multiple AFFECTED_ENTITIES entity ids") {
    val j =
      """{"DATA_SOURCE":"C","RECORD_ID":"1","AFFECTED_ENTITIES":[{"ENTITY_ID":10},{"ENTITY_ID":20}]}"""
    assert(InfoParser.affectedEntityIds(j) == Seq(10L, 20L))
  }

  test("empty or missing AFFECTED_ENTITIES yields no ids") {
    assert(InfoParser.affectedEntityIds("""{"AFFECTED_ENTITIES":[]}""").isEmpty)
    assert(InfoParser.affectedEntityIds("""{"DATA_SOURCE":"C","RECORD_ID":"1"}""").isEmpty)
  }

  test("malformed JSON raises loudly") {
    assertThrows[Exception](InfoParser.affectedEntityIds("{not json"))
  }

  test("parses RESOLVED_ENTITIES from a search result") {
    val j =
      """{"RESOLVED_ENTITIES":[{"ENTITY":{"RESOLVED_ENTITY":{"ENTITY_ID":42}}},{"ENTITY":{"RESOLVED_ENTITY":{"ENTITY_ID":43}}}]}"""
    assert(InfoParser.resolvedEntityIds(j) == Seq(42L, 43L))
    assert(InfoParser.resolvedEntityIds("""{"RESOLVED_ENTITIES":[]}""").isEmpty)
  }
}
