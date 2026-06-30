package com.senzing.spark.model

import org.scalatest.funsuite.AnyFunSuite

final class SchemasSpec extends AnyFunSuite {

  test("staging schema carries every union field") {
    val names = Schemas.staging.fieldNames.toSet
    val expected = Set(
      "kind",
      "dataSource",
      "recordId",
      "entityId",
      "op",
      "runId",
      "requestJson",
      "resultJson",
      "payload",
      "category",
      "errorCode",
      "message",
      "attempts"
    )
    assert(expected.subsetOf(names), s"missing: ${expected.diff(names)}")
  }

  test("affected entity id is a non-nullable Long") {
    val f = Schemas.affectedEntity("entityId")
    assert(f.dataType.typeName == "long")
    assert(!f.nullable)
  }

  test("staging entityId is nullable (union column)") {
    assert(Schemas.staging("entityId").nullable)
  }

  test("affected row round-trips through the staging union") {
    val a = AffectedEntityRow("CUSTOMERS", "1001", 42L, Op.Add, "run-1")
    assert(StagingRow.toAffected(StagingRow.of(a)) == a)
  }

  test("search row round-trips through the staging union") {
    val s = SearchResultRow("""{"NAME_FULL":"Jane Doe"}""", """{"RESOLVED_ENTITIES":[]}""")
    assert(StagingRow.toSearch(StagingRow.of(s)) == s)
  }

  test("error row round-trips through the staging union and exposes a stable dedup key") {
    val e = ErrorRow("CUSTOMERS", "1001", "{bad json", "BAD_INPUT", "SENZ0028", "boom", 1)
    assert(StagingRow.toError(StagingRow.of(e)) == e)
    assert(e.dedupKey == (("CUSTOMERS", "1001", "BAD_INPUT")))
  }

  test("staging kind is one of the three discriminators") {
    assert(StagingRow.of(SearchResultRow("a", "b")).kind == StagingKind.Search)
    assert(
      StagingRow.of(ErrorRow("d", "r", "p", "BAD_INPUT", "c", "m", 0)).kind == StagingKind.Error
    )
    assert(
      StagingRow.of(AffectedEntityRow("d", "r", 1L, Op.Delete, "run")).kind == StagingKind.Affected
    )
  }
}
