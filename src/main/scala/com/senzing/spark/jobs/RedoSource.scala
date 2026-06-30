package com.senzing.spark.jobs

/**
 * The single driver-side redo dequeuer. Pulls redo-record JSON via the injected `getRedoRecord`
 * supplier into a bounded batch that RedoJob turns into a DataFrame for parallel processing.
 *
 * The loop terminates on an empty dequeue or the batch limit — `countRedoRecords()` is NEVER used
 * as a loop condition (full table scan; monitoring only). One consumer of the global queue
 * side-steps the undemonstrated concurrent multi-consumer `getRedoRecord()` question. The supplier
 * returns the next record, or `null`/`""` when the queue is currently empty.
 */
object RedoSource {

  /**
   * Drain up to `limit` redo records. Does not over-pull: when the batch is full it stops without
   * an extra `getRedoRecord()` call.
   */
  def drainBatch(getRedoRecord: () => String, limit: Int): Seq[String] = {
    require(limit > 0, s"limit must be > 0, was $limit")
    val buf = scala.collection.mutable.ArrayBuffer.empty[String]
    var continue = true
    while (continue && buf.size < limit) {
      val r = getRedoRecord()
      if (r == null || r.isEmpty) continue = false
      else buf += r
    }
    buf.toSeq
  }
}
