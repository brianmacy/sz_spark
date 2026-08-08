package com.senzing.spark.mart

import org.scalatest.funsuite.AnyFunSuite

import com.senzing.spark.mart.EntityMartRows._

/**
 * Golden-file unit test for the pure [[EntityMartRows]] transform — the key correctness gate. No
 * Spark, no engine: it parses realistic `getEntity` JSON (the real v4 entity-read shape from the
 * Senzing-MCP `response_schemas` for the §7.4 flag set) and asserts the derived `entity` /
 * `entity_record` / `relationship` / `entity_doc` rows, the `lo < hi` relationship normalization,
 * tombstone-on-GONE, and hash stability/sensitivity.
 */
final class EntityMartRowsSpec extends AnyFunSuite {

  // Entity 100: three records across two sources, two representative features, two relations — one
  // to a higher id (100 is the `lo` side ⇒ forward match_key) and one to a lower id (100 is the `hi`
  // side ⇒ reverse match_key). IS_AMBIGUOUS / IS_DISCLOSED are 0/1 ints; USED_FOR_* are "Y"/"N".
  private val golden: String =
    """{
      |  "RESOLVED_ENTITY": {
      |    "ENTITY_ID": 100,
      |    "ENTITY_NAME": "Robert Smith",
      |    "FEATURES": {
      |      "NAME": [
      |        {"FEAT_DESC": "Robert Smith", "LIB_FEAT_ID": 11, "USAGE_TYPE": "PRIMARY",
      |         "FEAT_DESC_VALUES": [
      |           {"FEAT_DESC": "Robert Smith", "LIB_FEAT_ID": 11,
      |            "USED_FOR_CAND": "Y", "USED_FOR_SCORING": "Y"}]}
      |      ],
      |      "DOB": [
      |        {"FEAT_DESC": "1974-04-14", "LIB_FEAT_ID": 22,
      |         "FEAT_DESC_VALUES": [
      |           {"FEAT_DESC": "1974-04-14", "LIB_FEAT_ID": 22,
      |            "USED_FOR_CAND": "N", "USED_FOR_SCORING": "Y"}]}
      |      ]
      |    },
      |    "RECORD_SUMMARY": [
      |      {"DATA_SOURCE": "CUSTOMERS", "RECORD_COUNT": 2},
      |      {"DATA_SOURCE": "WATCHLIST", "RECORD_COUNT": 1}
      |    ],
      |    "RECORDS": [
      |      {"DATA_SOURCE": "CUSTOMERS", "RECORD_ID": "1001", "MATCH_KEY": "",
      |       "MATCH_LEVEL_CODE": "RESOLVED", "ERRULE_CODE": "",
      |       "FIRST_SEEN_DT": "2022-12-06 22:52:52.230", "LAST_SEEN_DT": "2022-12-06 22:52:52.230"},
      |      {"DATA_SOURCE": "CUSTOMERS", "RECORD_ID": "1002", "MATCH_KEY": "+NAME+DOB",
      |       "MATCH_LEVEL_CODE": "RESOLVED", "ERRULE_CODE": "SF1",
      |       "FIRST_SEEN_DT": "2022-12-07 09:10:00.000", "LAST_SEEN_DT": "2022-12-07 09:10:00.000"},
      |      {"DATA_SOURCE": "WATCHLIST", "RECORD_ID": "2001", "MATCH_KEY": "+NAME",
      |       "MATCH_LEVEL_CODE": "RESOLVED", "ERRULE_CODE": "SFF"}
      |    ]
      |  },
      |  "RELATED_ENTITIES": [
      |    {"ENTITY_ID": 200, "ENTITY_NAME": "Bob Smith", "MATCH_LEVEL_CODE": "POSSIBLY_RELATED",
      |     "MATCH_KEY": "+ADDRESS", "ERRULE_CODE": "CFF", "IS_AMBIGUOUS": 0, "IS_DISCLOSED": 0},
      |    {"ENTITY_ID": 50, "ENTITY_NAME": "R Smith", "MATCH_LEVEL_CODE": "POSSIBLY_SAME",
      |     "MATCH_KEY": "+NAME", "ERRULE_CODE": "SF1", "IS_AMBIGUOUS": 0, "IS_DISCLOSED": 1}
      |  ]
      |}""".stripMargin

  test("entity row: id, name, counts, record_summary, representative features") {
    val e = parseEntity(golden).entity
    assert(e.entity_id == 100L)
    assert(e.entity_name.contains("Robert Smith"))
    assert(e.record_count.contains(3)) // sum of RECORD_SUMMARY (2 + 1)
    assert(e.relation_count.contains(2))
    assert(e.record_summary == Map("CUSTOMERS" -> 2, "WATCHLIST" -> 1))

    val name = e.features.find(_.ftype_code == "NAME").getOrElse(fail("no NAME feature"))
    assert(name.lib_feat_id.contains(11L))
    assert(name.feat_desc.contains("Robert Smith"))
    assert(name.usage_type.contains("PRIMARY"))
    assert(name.used_for_cand.contains(true))
    assert(name.used_for_scoring.contains(true))

    val dob = e.features.find(_.ftype_code == "DOB").getOrElse(fail("no DOB feature"))
    assert(dob.used_for_cand.contains(false)) // "N"
    assert(dob.used_for_scoring.contains(true))
  }

  test("entity_record rows: the entity map, one per constituent record, all owned by entity 100") {
    val recs = parseEntity(golden).records
    assert(recs.size == 3)
    assert(recs.forall(_.entity_id == 100L))

    val r2 = recs.find(_.record_id == "1002").getOrElse(fail("missing record 1002"))
    assert(r2.data_source == "CUSTOMERS")
    assert(r2.match_key.contains("+NAME+DOB"))
    assert(r2.match_level_code.contains("RESOLVED"))
    assert(r2.errule_code.contains("SF1"))
    assert(r2.first_seen_dt.isDefined) // "2022-12-07 09:10:00.000" parsed to a Timestamp
    assert(r2.last_seen_dt.isDefined)

    // A record with no dates present ⇒ None, not a parse failure.
    val r3 = recs.find(_.record_id == "2001").getOrElse(fail("missing record 2001"))
    assert(r3.first_seen_dt.isEmpty && r3.last_seen_dt.isEmpty)
  }

  test("relationship rows: lo<hi normalization with forward/reverse match key placement") {
    val rels = parseEntity(golden).relationships
    assert(rels.size == 2)
    assert(rels.forall(r => r.entity_id_lo < r.entity_id_hi)) // never lo >= hi

    // 100 -> 200: this entity is the lo side ⇒ forward match_key, no reverse yet.
    val hi =
      rels.find(r => r.entity_id_lo == 100L && r.entity_id_hi == 200L).getOrElse(fail("100-200"))
    assert(hi.match_key.contains("+ADDRESS"))
    assert(hi.rev_match_key.isEmpty)
    assert(hi.match_level_code.contains("POSSIBLY_RELATED"))
    assert(hi.is_disclosed.contains(false))
    assert(hi.is_ambiguous.contains(false))

    // 100 -> 50: this entity is the hi side ⇒ the key is the REVERSE direction; forward is empty.
    val lo =
      rels.find(r => r.entity_id_lo == 50L && r.entity_id_hi == 100L).getOrElse(fail("50-100"))
    assert(lo.match_key.isEmpty)
    assert(lo.rev_match_key.contains("+NAME"))
    assert(lo.match_level_code.contains("POSSIBLY_SAME"))
    assert(lo.is_disclosed.contains(true)) // IS_DISCLOSED: 1
  }

  test("entity_doc row: the whole document by key, hash-tagged consistently with the entity row") {
    val p = parseEntity(golden)
    assert(p.doc.entity_id == 100L)
    assert(p.doc.entity_json == golden)
    assert(p.doc.entity_hash == p.entity.entity_hash) // same content hash on both projections
  }

  test("tombstoneOf: GONE ⇒ a tombstone; ENTITY / ERROR ⇒ none") {
    assert(tombstoneOf(GetResult(100L, GetKind.Gone, "", "", "", "")).contains(TombstoneRow(100L)))
    assert(tombstoneOf(GetResult(100L, GetKind.Entity, golden, "", "", "")).isEmpty)
    assert(tombstoneOf(GetResult(100L, GetKind.Error, "", "SYSTEMIC", "", "boom")).isEmpty)
  }

  test("hash is stable under element/key reordering and sensitive to content change") {
    // Same logical entity, but FEATURES/RECORDS/RELATED_ENTITIES orders and object-key order permuted.
    val reordered: String =
      """{
        |  "RELATED_ENTITIES": [
        |    {"IS_DISCLOSED": 1, "MATCH_KEY": "+NAME", "ENTITY_ID": 50, "ENTITY_NAME": "R Smith",
        |     "MATCH_LEVEL_CODE": "POSSIBLY_SAME", "ERRULE_CODE": "SF1", "IS_AMBIGUOUS": 0},
        |    {"ENTITY_ID": 200, "MATCH_KEY": "+ADDRESS", "MATCH_LEVEL_CODE": "POSSIBLY_RELATED",
        |     "ERRULE_CODE": "CFF", "IS_AMBIGUOUS": 0, "IS_DISCLOSED": 0, "ENTITY_NAME": "Bob Smith"}
        |  ],
        |  "RESOLVED_ENTITY": {
        |    "ENTITY_NAME": "Robert Smith",
        |    "RECORDS": [
        |      {"DATA_SOURCE": "WATCHLIST", "RECORD_ID": "2001", "MATCH_KEY": "+NAME",
        |       "MATCH_LEVEL_CODE": "RESOLVED", "ERRULE_CODE": "SFF"},
        |      {"MATCH_KEY": "+NAME+DOB", "DATA_SOURCE": "CUSTOMERS", "RECORD_ID": "1002",
        |       "MATCH_LEVEL_CODE": "RESOLVED", "ERRULE_CODE": "SF1",
        |       "FIRST_SEEN_DT": "2099-01-01 00:00:00.000", "LAST_SEEN_DT": "2099-01-01 00:00:00.000"},
        |      {"DATA_SOURCE": "CUSTOMERS", "RECORD_ID": "1001", "MATCH_KEY": "",
        |       "MATCH_LEVEL_CODE": "RESOLVED", "ERRULE_CODE": "",
        |       "FIRST_SEEN_DT": "2000-01-01 00:00:00.000", "LAST_SEEN_DT": "2000-01-01 00:00:00.000"}
        |    ],
        |    "RECORD_SUMMARY": [
        |      {"DATA_SOURCE": "WATCHLIST", "RECORD_COUNT": 1},
        |      {"DATA_SOURCE": "CUSTOMERS", "RECORD_COUNT": 2}
        |    ],
        |    "ENTITY_ID": 100,
        |    "FEATURES": {
        |      "DOB": [
        |        {"LIB_FEAT_ID": 22, "FEAT_DESC": "1974-04-14",
        |         "FEAT_DESC_VALUES": [
        |           {"USED_FOR_SCORING": "Y", "USED_FOR_CAND": "N", "LIB_FEAT_ID": 22,
        |            "FEAT_DESC": "1974-04-14"}]}
        |      ],
        |      "NAME": [
        |        {"USAGE_TYPE": "PRIMARY", "FEAT_DESC": "Robert Smith", "LIB_FEAT_ID": 11,
        |         "FEAT_DESC_VALUES": [
        |           {"USED_FOR_CAND": "Y", "USED_FOR_SCORING": "Y", "LIB_FEAT_ID": 11,
        |            "FEAT_DESC": "Robert Smith"}]}
        |      ]
        |    }
        |  }
        |}""".stripMargin

    // Reordering (incl. the churned seen-dates, which are deliberately excluded) ⇒ identical hash.
    assert(parseEntity(golden).entity.entity_hash == parseEntity(reordered).entity.entity_hash)

    // A real content change (entity name) ⇒ different hash.
    val renamed = golden.replace("\"Robert Smith\"", "\"Roberta Smith\"")
    assert(parseEntity(golden).entity.entity_hash != parseEntity(renamed).entity.entity_hash)
  }

  test("malformed entity JSON raises loudly; a document without ENTITY_ID is rejected") {
    assertThrows[Exception](parseEntity("{not json"))
    assertThrows[IllegalArgumentException](parseEntity("""{"RESOLVED_ENTITY":{}}"""))
  }
}
