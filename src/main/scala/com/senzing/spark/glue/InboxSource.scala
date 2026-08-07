package com.senzing.spark.glue

import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import com.senzing.spark.work.InputRecord

/**
 * Dispose-flavor [[RecordSource]] over the parquet inbox — the on-prem RabbitMQ path (the drainer
 * [[MqToParquet]] writes `part-<uuid>.parquet` shards; this hands them to the engine in overlapping
 * chunks). Claiming is an atomic rename (POSIX-only; see [[RecordSource]] for why this flavor does
 * not port to cloud object storage — Kafka/Delta use the watermark flavor there).
 *
 *   - `nextChunk` claims up to `filesPerChunk` inbox shards into a fresh `processing/<batchId>/`
 *     dir (rename = mutual exclusion) and returns them as ONE lazy DataFrame; `bounds` = the
 *     batchId.
 *   - `commit(batchId)` archives (or deletes) that dir — the chunk is now in the engine.
 *   - `reclaim` moves every shard left under `processing/` (nested batch dirs AND any flat
 *     leftovers) back to the inbox, so a crashed run's in-flight chunks are reprocessed
 *     (at-least-once).
 *
 * The chunk DataFrame is a lazy `spark.read.parquet(batchDir)` — Spark bin-packs the small shards
 * and the engine repartitions; data rides partitions to the executors, never the driver.
 */
final class InboxSource(
    spark: SparkSession,
    inbox: String,
    processing: String,
    archive: String,
    filesPerChunk: Int
) extends RecordSource {

  require(filesPerChunk > 0, s"filesPerChunk must be > 0, was $filesPerChunk")

  private val fs: FileSystem = ShardIo.fileSystem(spark, inbox)
  private val inboxPath = new Path(inbox)
  private val processingPath = new Path(processing)
  private val archiveOpt: Option[Path] = if (archive.nonEmpty) Some(new Path(archive)) else None
  private val schema = {
    import spark.implicits._
    spark.emptyDataset[InputRecord].schema
  }

  if (!fs.exists(processingPath)) fs.mkdirs(processingPath)

  def initialCursor: String = "" // dispose flavor has no cursor

  def nextChunk(cursor: String): Option[Chunk] = {
    val avail = ShardIo.listShards(fs, inboxPath).take(filesPerChunk)
    if (avail.isEmpty) None
    else {
      val batchId = UUID.randomUUID().toString
      val batchDir = new Path(processingPath, batchId)
      fs.mkdirs(batchDir)
      val claimed = avail.flatMap { p =>
        val dest = new Path(batchDir, p.getName)
        try if (fs.rename(p, dest)) Some(dest) else None
        catch { case _: java.io.FileNotFoundException => None }
      }
      if (claimed.isEmpty) { // every candidate lost the race / vanished
        fs.delete(batchDir, /*recursive=*/ true)
        None
      } else {
        import spark.implicits._
        val df = spark.read.schema(schema).parquet(batchDir.toString).as[InputRecord]
        Some(Chunk(bounds = batchId, df = df, nextCursor = ""))
      }
    }
  }

  def commit(bounds: String): Unit = {
    val batchDir = new Path(processingPath, bounds)
    archiveOpt match {
      case Some(a) =>
        if (!fs.exists(a)) fs.mkdirs(a)
        ShardIo.listShards(fs, batchDir).foreach { p =>
          if (!fs.rename(p, new Path(a, p.getName))) fs.delete(p, /*recursive=*/ false)
        }
        fs.delete(batchDir, /*recursive=*/ true)
      case None => fs.delete(batchDir, /*recursive=*/ true)
    }
  }

  def reclaim(): Unit = {
    if (!fs.exists(processingPath)) { fs.mkdirs(processingPath); return }
    // Flat leftovers (e.g. an older feeder's layout) straight back to the inbox.
    ShardIo
      .listShards(fs, processingPath)
      .foreach(p => fs.rename(p, new Path(inboxPath, p.getName)))
    // Nested batch dirs: move their shards back, then drop the empty dir.
    fs.listStatus(processingPath)
      .map(_.getPath)
      .filter(p => fs.getFileStatus(p).isDirectory)
      .foreach { batchDir =>
        ShardIo.listShards(fs, batchDir).foreach(p => fs.rename(p, new Path(inboxPath, p.getName)))
        fs.delete(batchDir, /*recursive=*/ true)
      }
  }
}
