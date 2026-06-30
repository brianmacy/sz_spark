package com.senzing.spark.jobs

import java.io.File
import java.nio.file.Files
import java.sql.DriverManager

import scala.jdk.CollectionConverters._

import com.senzing.sdk.core.SzCoreEnvironment

import com.senzing.spark.engine.SzEnvGuard
import com.senzing.spark.nativelib.EngineSettings

/**
 * One-time admin step (separate JVM, run once) — NEVER inside the Spark job, because two
 * `SzEnvironment` instances in one process corrupt the C library. It (1) applies the schema DDL via
 * plain JDBC, then (2) registers the default config using exactly ONE environment, then exits.
 *
 * Args: `db=<jdbcUrl>` `dialect=<postgresql|mysql|mssql|sqlite>` (schema step). The engine config
 * comes from `SENZING_ENGINE_CONFIGURATION_JSON`.
 */
object InitJob {

  /**
   * Greenfield vs existing: register+set a default config only when none exists. Returns the id in
   * force afterward. The DB advisory lock around this (closing the TOCTOU window) is applied by
   * `main`; this decision core is unit-tested.
   */
  def ensureDefaultConfig(getDefaultId: () => Long, registerAndSet: () => Long): Long = {
    val current = getDefaultId()
    if (current == 0L) registerAndSet() else current
  }

  def main(args: Array[String]): Unit = {
    val m =
      args.flatMap(_.split("=", 2) match { case Array(k, v) => Some(k -> v); case _ => None }).toMap
    val dialect = m.getOrElse("dialect", "postgresql")
    val senzingDir = sys.env.getOrElse("SENZING_DIR", "/opt/senzing")
    val dataSources = m.getOrElse("dataSources", "").split(",").map(_.trim).filter(_.nonEmpty)

    // (1) Schema DDL via plain JDBC — no SDK loaded for this step.
    (SchemaApplier.ddlFileName(dialect), m.get("db")) match {
      case (Some(fname), Some(jdbcUrl)) =>
        val conn = DriverManager.getConnection(jdbcUrl)
        try {
          val ddlFile = new File(s"$senzingDir/er/resources/schema/$fname")
          SchemaApplier.ensureSchema(
            () => SchemaApplier.tableExists(conn, "sys_cfg"),
            () => SchemaApplier.applyDdl(conn, Files.readString(ddlFile.toPath))
          )
        } finally conn.close()
      case _ => // SQLite (SDK auto-creates) or no JDBC url provided
    }

    // (2) Default config registration — exactly ONE environment, guarded.
    // NOTE: wrap in a DB advisory lock (pg_advisory_lock / lock row) in production to close TOCTOU.
    SzEnvGuard.markBuilt()
    val env = SzCoreEnvironment
      .newBuilder()
      .instanceName("sz-init")
      .settings(EngineSettings.fromEnv(System.getenv().asScala.toMap))
      .verboseLogging(false)
      .build()
    try {
      val cm = env.getConfigManager
      ensureDefaultConfig(
        () => cm.getDefaultConfigId,
        () => {
          val cfg = cm.createConfig() // from template
          dataSources.foreach(cfg.registerDataSource)
          cm.setDefaultConfig(cfg.export(), "initial config")
        }
      )
    } finally {
      env.destroy()
      SzEnvGuard.markDestroyed()
    }
  }
}
