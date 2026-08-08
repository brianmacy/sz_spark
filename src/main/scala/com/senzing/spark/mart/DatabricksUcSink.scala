package com.senzing.spark.mart

import org.apache.spark.sql.SparkSession

/**
 * The Databricks / Unity-Catalog target for the entity-mart serving map. It reuses
 * [[LocalDeltaSink]]'s exact MERGE/DELETE SQL, the `refresh_seq` monotonic guard, the relationship
 * `coalesce`, the change-gate and the orphan reconcile — everything in [[AbstractDeltaSink]] —
 * because the ONLY thing that differs between a local OSS-Delta table and a Databricks table is how
 * a mart table is NAMED: the local proxy uses a path identifier (`` delta.`/base/entity` ``); this
 * uses a Unity Catalog **three-part name** `catalog.schema.table`.
 *
 * The bookkeeping tables (`_sync_state`, `_quarantine`) begin with an underscore, so the table
 * segment is backtick-quoted to remain a valid UC identifier. `catalog`/`schema` are
 * caller-supplied and assumed to be valid UC identifiers.
 *
 * On Databricks the Delta SQL extensions + catalog are already configured on the cluster session,
 * so [[EntityMartSync]]'s `DeltaConf` is simply a redundant no-op there — the SAME assembled jar
 * runs unchanged; only the sink construction and the `mart=` argument shape differ (a
 * `catalog.schema` instead of a filesystem path).
 */
final class DatabricksUcSink(spark: SparkSession, catalog: String, schema: String)
    extends AbstractDeltaSink(spark) {

  protected def locator(table: String): String = DatabricksUcSink.ucLocator(catalog, schema, table)
}

object DatabricksUcSink {

  /**
   * A Unity Catalog three-part table name `` catalog.schema.`table` ``. The table part is
   * backtick-quoted so the leading-underscore bookkeeping tables (`_sync_state`/`_quarantine`) are
   * valid identifiers; `catalog`/`schema` are assumed already-valid UC identifiers.
   */
  def ucLocator(catalog: String, schema: String, table: String): String =
    s"$catalog.$schema.`$table`"

  /**
   * Parse the UC sink's `mart=` argument — a `catalog.schema` target — into its (catalog, schema)
   * pair. Raises loudly on a malformed value so a misconfigured launch fails fast instead of
   * writing to the wrong place. Exactly two non-empty dot-separated parts are required (the mart's
   * own tables live directly under that schema).
   */
  def parseTarget(catalogSchema: String): (String, String) =
    catalogSchema.split('.') match {
      case Array(c, s) if c.nonEmpty && s.nonEmpty => (c, s)
      case _ =>
        throw new IllegalArgumentException(
          s"UC sink expects a 'catalog.schema' target (exactly two non-empty parts); got: '$catalogSchema'"
        )
    }
}
