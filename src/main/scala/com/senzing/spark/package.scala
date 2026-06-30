package com.senzing

/**
 * sz_spark — a reference example calling the Senzing V4 Java SDK from Spark/Databricks against a
 * co-located SQL database, packaged as a self-extracting FAT jar.
 *
 * Layout (see docs/DESIGN.md, docs/IMPLEMENTATION_PLAN.md):
 *   - `nativelib` : FAT-jar native self-extraction + engine settings
 *   - `engine` : per-JVM SzEngine singleton + config-drift reinit
 *   - `work` : pure per-record worker, error taxonomy, parsing, progress
 *   - `jobs` : Spark job entry points (add/update, delete, search, redo) + one-time init
 *   - `model` : output/error DataFrame row types + schemas
 */
package object spark {
  final val Version: String = "0.1.0"
}
