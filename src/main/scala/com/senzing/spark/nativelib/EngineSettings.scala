package com.senzing.spark.nativelib

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

/**
 * Resolves `SENZING_ENGINE_CONFIGURATION_JSON` and, in FAT-jar mode, rewrites the three PIPELINE
 * paths to the extracted directories. Never hardcodes settings; never relies on `setupEnv`.
 */
object EngineSettings {

  final val EnvVar = "SENZING_ENGINE_CONFIGURATION_JSON"
  private val mapper = new ObjectMapper()

  /** Read the engine config from the environment; fail loudly if unset/blank. */
  def fromEnv(env: Map[String, String]): String =
    env
      .get(EnvVar)
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(throw new IllegalStateException(s"$EnvVar is not set (never hardcode it)"))

  /**
   * Rewrite the three PIPELINE keys to the extracted dirs (distinct trees). Creates PIPELINE if
   * absent. Leaves the SQL/other sections untouched.
   */
  def rewritePaths(json: String, paths: NativePaths): String = {
    val root = mapper.readTree(json)
    require(root != null && root.isObject, s"$EnvVar must be a JSON object")
    val obj = root.asInstanceOf[ObjectNode]
    val pipeline = obj.get("PIPELINE") match {
      case o: ObjectNode => o
      case _ =>
        val o = mapper.createObjectNode(); obj.set("PIPELINE", o); o
    }
    pipeline.put("SUPPORTPATH", paths.dataDir.getAbsolutePath)
    pipeline.put("RESOURCEPATH", paths.resourcesDir.getAbsolutePath)
    pipeline.put("CONFIGPATH", paths.configDir.getAbsolutePath)
    mapper.writeValueAsString(obj)
  }

  /** PGSSLMODE for cloud Postgres (libpq reads it); default `require` if unset. */
  def pgSslMode(env: Map[String, String]): String =
    env.get("PGSSLMODE").map(_.trim).filter(_.nonEmpty).getOrElse("require")
}
