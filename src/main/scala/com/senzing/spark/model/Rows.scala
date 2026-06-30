package com.senzing.spark.model

/** Operation tag carried on an affected-entity output row. */
object Op {
  final val Add: String = "ADD" // add/update use the same Senzing verb
  final val Delete: String = "DELETE"
  final val Redo: String = "REDO"
}

/** Tagged-union discriminator for the single staging write (see SparkRecordOps, option B). */
object StagingKind {
  final val Affected: String = "AFFECTED"
  final val Search: String = "SEARCH"
  final val Error: String = "ERROR"
}

/**
 * One affected entity id from a mutating verb's WITH_INFO `AFFECTED_ENTITIES`. `op` is one of
 * [[Op]]; `runId` ties rows to a single job run.
 */
final case class AffectedEntityRow(
    dataSource: String,
    recordId: String,
    entityId: Long,
    op: String,
    runId: String
)

/** A search request paired with its raw result JSON. */
final case class SearchResultRow(requestJson: String, resultJson: String)

/**
 * A record that failed processing, routed to the error DataFrame. `category` is an
 * [[com.senzing.spark.work.ErrorCategory]] name; `errorCode` is the Senzing code when available.
 * The stable dedup key is (dataSource, recordId, category) — see [[dedupKey]].
 */
final case class ErrorRow(
    dataSource: String,
    recordId: String,
    payload: String,
    category: String,
    errorCode: String,
    message: String,
    attempts: Int
) {
  def dedupKey: (String, String, String) = (dataSource, recordId, category)
}

/**
 * The single tagged-union row written by the one engine-calling pass. Exactly one of the good kinds
 * (AFFECTED/SEARCH) or ERROR is populated per row; the reader splits by `kind`. Nullable columns
 * are `Option` so Spark encodes them as nullable.
 */
final case class StagingRow(
    kind: String,
    dataSource: Option[String] = None,
    recordId: Option[String] = None,
    entityId: Option[Long] = None,
    op: Option[String] = None,
    runId: Option[String] = None,
    requestJson: Option[String] = None,
    resultJson: Option[String] = None,
    payload: Option[String] = None,
    category: Option[String] = None,
    errorCode: Option[String] = None,
    message: Option[String] = None,
    attempts: Option[Int] = None
)

object StagingRow {
  def of(r: AffectedEntityRow): StagingRow =
    StagingRow(
      kind = StagingKind.Affected,
      dataSource = Some(r.dataSource),
      recordId = Some(r.recordId),
      entityId = Some(r.entityId),
      op = Some(r.op),
      runId = Some(r.runId)
    )

  def of(r: SearchResultRow): StagingRow =
    StagingRow(
      kind = StagingKind.Search,
      requestJson = Some(r.requestJson),
      resultJson = Some(r.resultJson)
    )

  def of(r: ErrorRow): StagingRow =
    StagingRow(
      kind = StagingKind.Error,
      dataSource = Some(r.dataSource),
      recordId = Some(r.recordId),
      payload = Some(r.payload),
      category = Some(r.category),
      errorCode = Some(r.errorCode),
      message = Some(r.message),
      attempts = Some(r.attempts)
    )

  def toAffected(s: StagingRow): AffectedEntityRow =
    AffectedEntityRow(s.dataSource.get, s.recordId.get, s.entityId.get, s.op.get, s.runId.get)

  def toSearch(s: StagingRow): SearchResultRow =
    SearchResultRow(s.requestJson.get, s.resultJson.get)

  def toError(s: StagingRow): ErrorRow =
    ErrorRow(
      s.dataSource.get,
      s.recordId.get,
      s.payload.get,
      s.category.get,
      s.errorCode.get,
      s.message.get,
      s.attempts.get
    )
}
