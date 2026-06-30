package com.senzing.spark.model

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType

/**
 * Spark `StructType`s for the output/error/staging rows, derived from the case classes so the
 * schema can never drift from the code. No `SparkSession` is required to read these.
 */
object Schemas {
  val affectedEntity: StructType = Encoders.product[AffectedEntityRow].schema
  val searchResult: StructType = Encoders.product[SearchResultRow].schema
  val error: StructType = Encoders.product[ErrorRow].schema
  val staging: StructType = Encoders.product[StagingRow].schema
}
