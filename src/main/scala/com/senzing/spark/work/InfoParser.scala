package com.senzing.spark.work

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

/**
 * Parses Senzing WITH_INFO responses and search results. Tolerant of empty/missing
 * `AFFECTED_ENTITIES`; malformed JSON raises loudly (a parse failure is a real defect, not a skip).
 */
object InfoParser {

  private val mapper = new ObjectMapper()

  /** Entity ids from a mutating verb's WITH_INFO `AFFECTED_ENTITIES[].ENTITY_ID`. */
  def affectedEntityIds(withInfoJson: String): Seq[Long] = {
    val root = mapper.readTree(withInfoJson) // throws on malformed JSON — intentional
    val arr = root.path("AFFECTED_ENTITIES")
    if (!arr.isArray) Seq.empty
    else
      arr.elements().asScala.toSeq.flatMap { n =>
        val e = n.path("ENTITY_ID")
        if (e.isIntegralNumber) Some(e.asLong) else None
      }
  }

  /** Resolved entity ids from a `searchByAttributes` result's `RESOLVED_ENTITIES[]`. */
  def resolvedEntityIds(searchJson: String): Seq[Long] = {
    val root = mapper.readTree(searchJson)
    val arr = root.path("RESOLVED_ENTITIES")
    if (!arr.isArray) Seq.empty
    else
      arr.elements().asScala.toSeq.flatMap { n =>
        val id: JsonNode = n.path("ENTITY").path("RESOLVED_ENTITY").path("ENTITY_ID")
        if (id.isIntegralNumber) Some(id.asLong) else None
      }
  }
}
