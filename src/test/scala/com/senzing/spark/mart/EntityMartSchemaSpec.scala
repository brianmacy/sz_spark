package com.senzing.spark.mart

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit test for the generated mart DDL — no Spark. Pins the per-sink differences: Unity Catalog
 * gets informational PRIMARY KEY / FOREIGN KEY constraints; the path-based local sink gets none
 * (OSS Spark cannot parse them). Also pins the VARIANT `entity_doc.entity_json` and that every
 * table and column carries a COMMENT.
 */
final class EntityMartSchemaSpec extends AnyFunSuite {

  private def uc(table: String): String = DatabricksUcSink.tableDdl("c", "s", table)
  private def local(table: String): String = LocalDeltaSink.tableDdl("/m", table)

  test("UC DDL declares the informational primary keys (RELY: every write is keyed on them)") {
    assert(uc("entity").contains("CONSTRAINT entity_pk PRIMARY KEY (entity_id) RELY"))
    assert(
      uc("entity_record")
        .contains("CONSTRAINT entity_record_pk PRIMARY KEY (data_source, record_id) RELY")
    )
    assert(
      uc("relationship")
        .contains("CONSTRAINT relationship_pk PRIMARY KEY (entity_id_lo, entity_id_hi) RELY")
    )
  }

  test("UC DDL declares the informational foreign keys to entity (NOT ENFORCED, NORELY)") {
    val parent = "c.s.`entity` (entity_id)"
    assert(
      uc("entity_record").contains(
        s"CONSTRAINT entity_record_entity_fk FOREIGN KEY (entity_id) REFERENCES $parent NOT ENFORCED NORELY"
      )
    )
    val rel = uc("relationship")
    assert(
      rel.contains(
        s"CONSTRAINT relationship_lo_fk FOREIGN KEY (entity_id_lo) REFERENCES $parent NOT ENFORCED NORELY"
      )
    )
    assert(
      rel.contains(
        s"CONSTRAINT relationship_hi_fk FOREIGN KEY (entity_id_hi) REFERENCES $parent NOT ENFORCED NORELY"
      )
    )
  }

  test("UC DDL adds no constraints to entity_doc or the bookkeeping tables") {
    for (t <- Seq("entity_doc", "_sync_state", "_quarantine"))
      withClue(s"$t: ")(assert(!uc(t).contains("CONSTRAINT")))
  }

  test("the path-based local DDL has no constraints at all") {
    for (t <- EntityMartSchema.Tables)
      withClue(s"$t: ") {
        val ddl = local(t)
        assert(!ddl.contains("CONSTRAINT"))
        assert(!ddl.contains("PRIMARY KEY"))
        assert(!ddl.contains("FOREIGN KEY"))
        assert(ddl.startsWith("CREATE TABLE IF NOT EXISTS delta.`/m/" + t + "`"))
      }
  }

  test("entity_doc.entity_json is VARIANT on both sinks") {
    assert(local("entity_doc").matches("(?s).*\\bentity_json\\s+VARIANT\\b.*"))
    assert(uc("entity_doc").matches("(?s).*\\bentity_json\\s+VARIANT\\b.*"))
  }

  test("every table and every column carries a COMMENT") {
    for (t <- EntityMartSchema.Tables) {
      val ddl = local(t)
      withClue(s"$t table comment: ")(assert(ddl.contains("\nCOMMENT '")))
      val cols = EntityMartSchema.columnNames(t)
      assert(cols.nonEmpty)
      for (c <- cols)
        withClue(s"$t.$c: ") {
          assert(
            ddl.linesIterator.exists(l => l.trim.startsWith(c + " ") && l.contains(" COMMENT '"))
          )
        }
    }
  }

  test("the entity_doc VARIANT migration converts the column in place") {
    val loc = "delta.`/m/entity_doc`"
    assert(
      EntityMartSchema.entityDocVariantMigration(loc) == Seq(
        s"ALTER TABLE $loc SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', " +
          "'delta.feature.variantType' = 'supported')",
        s"ALTER TABLE $loc ADD COLUMN entity_json_variant VARIANT COMMENT " +
          "'Full getEntity response document'",
        s"UPDATE $loc SET entity_json_variant = parse_json(entity_json)",
        s"ALTER TABLE $loc DROP COLUMN entity_json",
        s"ALTER TABLE $loc RENAME COLUMN entity_json_variant TO entity_json"
      )
    )
  }
}
