package com.senzing.spark.glue

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.UUID

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Durable, out-of-order-safe monotonic watermark over a single contiguous offset stream — the
 * bookkeeping the watermark-flavor [[RecordSource]]s (Kafka offset, Delta version) need but the
 * dispose flavor ([[InboxSource]]) does not.
 *
 * WHY OUT-OF-ORDER MATTERS: [[OverlappingBatchEngine]] claims ranges strictly in order (its `claim`
 * is serialized and threads `nextCursor`), so the claimed ranges are contiguous and non-overlapping
 * — `[c0,c1) [c1,c2) [c2,c3) …`. But `commit(bounds)` is called from K worker threads and therefore
 * completes '''out of order''': a straggler on `[c1,c2)` can finish long after `[c2,c3)`. A
 * monotonic cursor may only advance over the '''contiguous-completed prefix''', so this class holds
 * the completed-but-not-yet-contiguous ranges until the gap in front of them fills, then advances
 * and persists in one step.
 *
 * DURABILITY: the committed offset is persisted to `checkpointFile` on every advance,
 * double-buffered (`checkpointFile` + a `.bak` sibling) so a crash mid-write still leaves at least
 * a one-batch-behind value to resume from — the tail it replays is a handful of already-resolved
 * records, each a cheap optimized no-op re-add (at-least-once). A restart re-reads from the
 * committed offset; there is no reclaim (that is the dispose flavor's job).
 *
 * All mutating ops are `synchronized` — `complete` is called concurrently by the engine's workers.
 */
final class OffsetWatermark(fs: FileSystem, checkpointFile: Path, start: Long) {

  private val bakFile = new Path(checkpointFile.getParent, checkpointFile.getName + ".bak")

  // Completed ranges whose start is at or ahead of the committed frontier, keyed by start offset.
  // Contiguous-by-construction, so advancing is "is there a range starting exactly at committed?".
  private val completedByStart = mutable.Map.empty[Long, Long]

  private var committed: Long = OffsetWatermark.load(fs, checkpointFile, bakFile).getOrElse(start)

  /** The durably-committed low-water offset: where a restart resumes reading. */
  def committedOffset: Long = synchronized(committed)

  /**
   * Record that the range `[startOff, endOff)` finished successfully, then advance the committed
   * frontier over every now-contiguous completed range and persist if it moved. Idempotent: a range
   * already behind the frontier (a replay re-completion) is ignored.
   */
  def complete(startOff: Long, endOff: Long): Unit = synchronized {
    require(endOff >= startOff, s"range end $endOff < start $startOff")
    if (startOff < committed) return // already advanced past this range (replay) — ignore
    completedByStart(startOff) = math.max(endOff, completedByStart.getOrElse(startOff, endOff))
    var moved = false
    var next = completedByStart.remove(committed)
    while (next.isDefined) {
      committed = next.get
      moved = true
      next = completedByStart.remove(committed)
    }
    if (moved) persist(committed)
  }

  /** Atomic double-buffered write: bak <- old primary, primary <- new value. */
  private def persist(off: Long): Unit = {
    val dir = checkpointFile.getParent
    if (dir != null && !fs.exists(dir)) fs.mkdirs(dir)
    val tmp = new Path(dir, s".${checkpointFile.getName}.tmp-${UUID.randomUUID()}")
    val os = fs.create(tmp, /*overwrite=*/ true)
    try os.write(off.toString.getBytes(StandardCharsets.UTF_8))
    finally os.close()
    // Roll the current primary down to .bak first, so the tmp->primary rename lands on an empty
    // name (Hadoop rename does not overwrite) AND a crash still leaves .bak one-behind to recover.
    if (fs.exists(checkpointFile)) {
      fs.delete(bakFile, /*recursive=*/ false)
      fs.rename(checkpointFile, bakFile)
    }
    if (!fs.rename(tmp, checkpointFile)) {
      fs.delete(tmp, /*recursive=*/ false)
      throw new IOException(s"watermark checkpoint rename failed: $tmp -> $checkpointFile")
    }
  }
}

object OffsetWatermark {

  /** Read the committed offset from the primary checkpoint, falling back to `.bak`, else `None`. */
  private[glue] def load(fs: FileSystem, primary: Path, bak: Path): Option[Long] =
    readOffset(fs, primary).orElse(readOffset(fs, bak))

  private def readOffset(fs: FileSystem, p: Path): Option[Long] =
    try
      if (!fs.exists(p)) None
      else {
        val is = fs.open(p)
        try {
          val bytes = new Array[Byte](64)
          val n = is.read(bytes)
          if (n <= 0) None
          else new String(bytes, 0, n, StandardCharsets.UTF_8).trim.toLongOption
        } finally is.close()
      }
    catch { case NonFatal(_) => None }
}
