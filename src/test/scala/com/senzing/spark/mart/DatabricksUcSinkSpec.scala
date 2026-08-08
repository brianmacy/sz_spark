package com.senzing.spark.mart

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit test for [[DatabricksUcSink]]'s pure locator + target-parse logic — the ONLY behavior that
 * differs from [[LocalDeltaSink]] (all MERGE/DELETE/change-gate/reconcile logic is the shared,
 * IT-proven [[AbstractDeltaSink]], so it does not need re-testing per sink). No Spark, no Unity
 * Catalog.
 */
final class DatabricksUcSinkSpec extends AnyFunSuite {

  test(
    "ucLocator builds a three-part name and backtick-quotes the table (leading-underscore safe)"
  ) {
    assert(
      DatabricksUcSink.ucLocator("main", "entity_mart", "entity") == "main.entity_mart.`entity`"
    )
    assert(
      DatabricksUcSink.ucLocator(
        "main",
        "entity_mart",
        "relationship"
      ) == "main.entity_mart.`relationship`"
    )
    // bookkeeping tables begin with '_' — the backtick keeps them valid UC identifiers
    assert(DatabricksUcSink.ucLocator("cat", "sch", "_sync_state") == "cat.sch.`_sync_state`")
    assert(DatabricksUcSink.ucLocator("cat", "sch", "_quarantine") == "cat.sch.`_quarantine`")
  }

  test("every EntityMartSchema table yields a distinct three-part locator under the schema") {
    val locs = EntityMartSchema.Tables.map(t => DatabricksUcSink.ucLocator("c", "s", t))
    assert(locs.distinct.size == EntityMartSchema.Tables.size)
    assert(locs.forall(_.startsWith("c.s.`")))
  }

  test("parseTarget splits a valid catalog.schema") {
    assert(DatabricksUcSink.parseTarget("main.entity_mart") == (("main", "entity_mart")))
  }

  test("parseTarget rejects malformed targets loudly (fail-fast on misconfig)") {
    for (bad <- Seq("just_one", "a.b.c", "", ".", "a.", ".b"))
      withClue(s"target='$bad': ") {
        assertThrows[IllegalArgumentException](DatabricksUcSink.parseTarget(bad))
      }
  }
}
