package com.senzing.spark.glue

import org.apache.spark.sql.Dataset

import com.senzing.spark.work.InputRecord

/**
 * One unit of work handed to [[OverlappingBatchEngine]]: an opaque `bounds` (what `commit` later
 * disposes/watermarks), the LAZY `df` to process (record data flows through Spark partitions to the
 * executors — never through the driver), and `nextCursor` (the source cursor AFTER this chunk —
 * meaningful for watermark sources, ignored/empty for the dispose flavor).
 */
final case class Chunk(bounds: String, df: Dataset[InputRecord], nextCursor: String)

/**
 * Pluggable source for the source-agnostic [[OverlappingBatchEngine]]. "The parallel-batch overlap
 * is the mechanism; the source can be anything" — RabbitMQ (via the drainer + parquet inbox),
 * Kafka, a Delta table, files. RabbitMQ needs the inbox adapter only because it has no replayable
 * cursor; cursor-native sources skip it.
 *
 * TWO COMMIT FLAVORS (this is what makes the engine portable off POSIX onto cloud object storage):
 *   - '''dispose''' (POSIX / RabbitMQ-via-inbox, [[InboxSource]]): `commit` archives/deletes the
 *     claimed files; `reclaim` moves in-flight files back to the inbox. Atomic-rename based ⇒
 *     on-prem only (rename is copy+delete and non-atomic on S3/ADLS/GCS).
 *   - '''monotonic watermark''' (Kafka offset / Delta version — Step 2): `commit` advances the
 *     contiguous-completed cursor; `reclaim` is a no-op (restart re-reads from the committed
 *     cursor, replay being a cheap optimized no-op re-add). Object-store-safe ⇒ the
 *     Databricks-native path.
 *
 * `nextChunk` / `commit` / `reclaim` are METADATA ops on the driver; the returned `df` is lazy so
 * all record data flows through Spark partitions.
 */
trait RecordSource {

  /**
   * The cursor to start from: a committed watermark, or `""` for the dispose flavor (no cursor).
   */
  def initialCursor: String

  /**
   * Claim/read the next chunk from `cursor`, or `None` when nothing is available right now.
   * Claiming is metadata-only (a rename for dispose, an offset-range pick for watermark); the
   * returned `df` is lazy. Called serially by the engine so the cursor advances safely.
   */
  def nextChunk(cursor: String): Option[Chunk]

  /** Commit a successfully-processed chunk: dispose the claimed files, or advance the watermark. */
  def commit(bounds: String): Unit

  /**
   * Recover a prior run's in-flight work before claiming: dispose flavor moves the `processing`
   * dir's shards back to the inbox; watermark flavor is a no-op (restart re-reads from the
   * committed cursor).
   */
  def reclaim(): Unit
}
