package com.senzing.spark.jobs

import java.sql.Connection

/**
 * Applies the Senzing schema DDL via plain JDBC (NOT the SDK) — a one-time admin step. Dialect-
 * branched and idempotent: skip if `sys_cfg` already exists. SQLite is special — the SDK
 * auto-creates the file during config registration, so there is no DDL step.
 */
object SchemaApplier {

  /** DDL file under `resources/schema/`, or `None` for SQLite (SDK auto-creates). */
  def ddlFileName(dialect: String): Option[String] = dialect.toLowerCase match {
    case "postgresql" | "postgres" => Some("szcore-schema-postgresql-create.sql")
    case "mysql" => Some("szcore-schema-mysql-create.sql")
    case "mssql" | "sqlserver" => Some("szcore-schema-mssql-create.sql")
    case "sqlite" => None
    case other => throw new IllegalArgumentException(s"Unsupported DB dialect: $other")
  }

  /** Pure orchestration: apply only when absent. Returns true iff DDL was applied. */
  def ensureSchema(schemaPresent: () => Boolean, applyDdl: () => Unit): Boolean =
    if (schemaPresent()) false
    else { applyDdl(); true }

  // ---- JDBC helpers (integration; covered by InitIT) ----

  def tableExists(conn: Connection, table: String): Boolean = {
    val rs = conn.getMetaData.getTables(null, null, table, Array("TABLE"))
    try rs.next()
    finally rs.close()
  }

  def applyDdl(conn: Connection, ddl: String): Unit = {
    val st = conn.createStatement()
    try ddl.split(";").map(_.trim).filter(_.nonEmpty).foreach(st.execute)
    finally st.close()
  }
}
