package com.senzing.spark.core

import com.senzing.sdk.{SzEngine, SzFlag, SzRecordKey}

import com.senzing.spark.transform.RecordTransform
import com.senzing.spark.work.InputRecord

/**
 * The Senzing verbs as `SzEngine => (InputRecord => String)` functions, all requesting
 * `SZ_WITH_INFO` so the response carries `AFFECTED_ENTITIES`. Signatures verified by `javap` on the
 * installed jar: `addRecord(SzRecordKey,String,Set<SzFlag>)`,
 * `deleteRecord(SzRecordKey,Set<SzFlag>)`, `searchByAttributes(String)`,
 * `processRedoRecord(String,Set<SzFlag>)`.
 */
object Verbs {
  private val WithInfo: java.util.Set[SzFlag] = SzFlag.SZ_WITH_INFO_FLAGS

  /**
   * Add/update. With a record-transform plugin the payload is rewritten just before `addRecord`;
   * the key stays the ORIGINAL record's (DATA_SOURCE / RECORD_ID), so the plugin only ever adds or
   * rewrites features. Redo, delete and search are never transformed.
   */
  def add(e: SzEngine, transform: Option[RecordTransform] = None): InputRecord => String =
    r =>
      e.addRecord(
        SzRecordKey.of(r.dataSource, r.recordId),
        transform.flatMap(_(r.payload)).getOrElse(r.payload),
        WithInfo
      )

  def delete(e: SzEngine): InputRecord => String =
    r => e.deleteRecord(SzRecordKey.of(r.dataSource, r.recordId), WithInfo)

  def search(e: SzEngine): InputRecord => String =
    r => e.searchByAttributes(r.payload)

  /** For redo, the record payload IS the redo-record JSON returned by getRedoRecord(). */
  def redo(e: SzEngine): InputRecord => String =
    r => e.processRedoRecord(r.payload, WithInfo)
}
