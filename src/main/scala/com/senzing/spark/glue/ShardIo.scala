package com.senzing.spark.glue

import java.util.UUID

import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * Filesystem shard primitives shared by the glue jobs: atomic single-file parquet writes and the
 * claim / reclaim / dispose moves the [[ParquetParallelFeeder]] uses to own inbox shards without a
 * lock across processes. Factored out of [[MqToParquet]] (which now delegates its shard write here)
 * so the write-ahead atomic-rename discipline lives in exactly one place.
 *
 * Shard-file convention (shared with `MqToParquet`): a real shard is a NON-dot, non-`_` `.parquet`
 * leaf directly under a dir (`part-<uuid>.parquet`, `de-<uuid>.parquet`, ...). Writers stage under
 * a dot-prefixed `.tmp-<uuid>` dir so readers listing the dir never see a half-written footer, then
 * atomically rename the single leaf out — the whole file appears at once.
 */
object ShardIo {

  /** The Hadoop `FileSystem` for `path` under the session's config. */
  def fileSystem(spark: SparkSession, path: String): FileSystem =
    new Path(path).getFileSystem(spark.sparkContext.hadoopConfiguration)

  /**
   * A real shard leaf: `*.parquet`, not a dot-file (`.tmp-`, `.crc`) or a `_SUCCESS`/`_temporary`.
   */
  private def isShardFile(p: Path): Boolean = {
    val n = p.getName
    n.endsWith(".parquet") && !n.startsWith(".") && !n.startsWith("_")
  }

  /**
   * The `part-*.parquet` (or other prefix) shard files directly under `dir`; empty if `dir` absent.
   */
  def listShards(fs: FileSystem, dir: Path): Array[Path] =
    if (!fs.exists(dir)) Array.empty
    else fs.listStatus(dir).map(_.getPath).filter(isShardFile)

  /**
   * Write `df` as ONE parquet file `dir/<prefix>-<uuid>.parquet`, appearing atomically: coalesce to
   * a single leaf under `dir/.tmp-<uuid>`, rename the leaf to its final name, drop the staging dir.
   * Returns the final `Path`, or `None` when the frame produced no data leaf (0 rows) — the caller
   * decides whether an empty frame is an error (the drainer) or a skip (the feeder's sinks).
   */
  def writeSingleFile(
      spark: SparkSession,
      df: DataFrame,
      dir: String,
      prefix: String
  ): Option[Path] = {
    val uuid = UUID.randomUUID().toString
    val dirPath = new Path(dir)
    val tmp = new Path(dirPath, s".tmp-$uuid")
    val fin = new Path(dirPath, s"$prefix-$uuid.parquet")
    df.coalesce(1).write.mode(SaveMode.Overwrite).parquet(tmp.toString)
    val fs = dirPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    try
      fs.listStatus(tmp).map(_.getPath).find(isShardFile) match {
        case Some(leaf) =>
          if (!fs.rename(leaf, fin))
            throw new java.io.IOException(s"atomic shard rename failed: $leaf -> $fin")
          Some(fin)
        case None => None // 0-row frame: single partition wrote no data file
      }
    finally fs.delete(tmp, /*recursive=*/ true) // drop _SUCCESS/.crc staging remnants
  }

  /**
   * Atomic claim: `rename(inbox/part-X → processing/part-X)`. The rename IS the mutual exclusion —
   * a losing racer (or an already-moved shard) gets `false`/`FileNotFound` and the caller tries the
   * next. Returns the claimed destination path on success.
   */
  def claim(fs: FileSystem, shard: Path, processingDir: Path): Option[Path] = {
    if (!fs.exists(processingDir)) fs.mkdirs(processingDir)
    val dest = new Path(processingDir, shard.getName)
    try if (fs.rename(shard, dest)) Some(dest) else None
    catch { case _: java.io.FileNotFoundException => None }
  }

  /**
   * Move every shard left in `processingDir` (a prior run's in-flight units) back to `inboxDir`.
   */
  def reclaim(fs: FileSystem, processingDir: Path, inboxDir: Path): Int = {
    if (!fs.exists(inboxDir)) fs.mkdirs(inboxDir)
    listShards(fs, processingDir).foldLeft(0) { (n, p) =>
      if (fs.rename(p, new Path(inboxDir, p.getName))) n + 1 else n
    }
  }

  /**
   * Dispose a completed shard: archive it (rename into `archiveDir`) when one is configured, else
   * delete it. Archive is best-effort — a rename failure falls back to delete so a completed shard
   * is NEVER left in `processing/` (which would re-reclaim + reprocess it on the next restart).
   */
  def dispose(fs: FileSystem, claimed: Path, archiveDir: Option[Path]): Unit = archiveDir match {
    case Some(a) =>
      if (!fs.exists(a)) fs.mkdirs(a)
      if (!fs.rename(claimed, new Path(a, claimed.getName)))
        fs.delete(claimed, /*recursive=*/ false)
    case None => fs.delete(claimed, /*recursive=*/ false)
  }

  /** Best-effort recursive delete (per-unit staging cleanup); never fails the caller. */
  def deleteQuietly(fs: FileSystem, p: Path): Unit =
    try fs.delete(p, /*recursive=*/ true)
    catch { case NonFatal(_) => () }

  /**
   * Serialized inbox claimer shared by the feeder's K worker threads: a `synchronized` `claimNext`
   * buffers a directory listing and hands out atomically-claimed shards one at a time (the "single
   * reader"). Refills the buffer from the inbox when it empties, so shards the drainer writes after
   * start are picked up. Returns `None` only when the inbox is currently empty.
   */
  final class Claimer(fs: FileSystem, inbox: Path, processing: Path) {
    private val pending = scala.collection.mutable.Queue.empty[Path]

    def claimNext(): Option[Path] = synchronized {
      var result: Option[Path] = None
      var done = false
      while (!done) {
        if (pending.isEmpty) listShards(fs, inbox).foreach(pending.enqueue(_))
        if (pending.isEmpty) done = true // inbox empty
        else
          claim(fs, pending.dequeue(), processing) match {
            case some @ Some(_) => result = some; done = true
            case None => () // lost/gone — try the next candidate
          }
      }
      result
    }
  }
}
