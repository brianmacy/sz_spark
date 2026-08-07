package com.senzing.spark.glue

import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Exercises the out-of-order-safe, durable contiguous-prefix watermark on a REAL local Hadoop
 * filesystem (the same `LocalFileSystem` class the on-prem fleet's checkpoint dir uses) — no mocks.
 * The engine claims ranges in order but its K workers COMMIT them out of order, so the invariants
 * under test are: (1) the durable offset advances only over the contiguous-completed prefix, (2) a
 * gap (a failed/straggler range) holds the frontier until it fills, (3) completes are idempotent
 * and replays behind the frontier are ignored, and (4) the offset survives a restart and a lost
 * primary checkpoint (double-buffer fallback).
 */
class OffsetWatermarkSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var fs: FileSystem = _
  private var dir: Path = _

  override def beforeAll(): Unit = {
    fs = FileSystem.getLocal(new Configuration())
    dir = new Path("file://" + Files.createTempDirectory("sz-watermark").toString)
  }

  override def afterAll(): Unit = if (fs != null) fs.close()

  private def cpFile(name: String): Path = new Path(dir, name)

  test("cold start with no checkpoint reports the configured start offset") {
    val wm = new OffsetWatermark(fs, cpFile("cold"), start = 42L)
    assert(wm.committedOffset == 42L)
  }

  test("in-order completes advance the frontier and persist across a restart") {
    val f = cpFile("inorder")
    val wm = new OffsetWatermark(fs, f, start = 0L)
    wm.complete(0L, 1000L)
    assert(wm.committedOffset == 1000L)
    wm.complete(1000L, 2000L)
    assert(wm.committedOffset == 2000L)
    // A fresh instance over the same checkpoint file resumes at the persisted offset.
    val restarted = new OffsetWatermark(fs, f, start = 0L)
    assert(restarted.committedOffset == 2000L)
  }

  test("an out-of-order complete is held until the range in front of it fills") {
    val wm = new OffsetWatermark(fs, cpFile("ooo"), start = 0L)
    wm.complete(1000L, 2000L) // arrives first, but [0,1000) is still open
    assert(wm.committedOffset == 0L, "frontier must not jump over the still-open [0,1000)")
    wm.complete(0L, 1000L) // fills the gap → both become contiguous
    assert(wm.committedOffset == 2000L)
  }

  test("a gap (a failed/straggler range) holds the frontier, then releases when filled") {
    val wm = new OffsetWatermark(fs, cpFile("gap"), start = 0L)
    wm.complete(0L, 1000L)
    wm.complete(2000L, 3000L) // [1000,2000) never completed (a straggler/failure)
    assert(wm.committedOffset == 1000L, "must not advance past the missing [1000,2000)")
    wm.complete(3000L, 4000L) // still gated behind the gap
    assert(wm.committedOffset == 1000L)
    wm.complete(1000L, 2000L) // gap fills → frontier sweeps all contiguous ranges at once
    assert(wm.committedOffset == 4000L)
  }

  test("completes are idempotent and replays behind the frontier are ignored") {
    val wm = new OffsetWatermark(fs, cpFile("idem"), start = 0L)
    wm.complete(0L, 1000L)
    wm.complete(0L, 1000L) // duplicate delivery — no throw, no double-advance
    assert(wm.committedOffset == 1000L)
    wm.complete(0L, 1000L) // a restart replay of an already-committed range — ignored
    assert(wm.committedOffset == 1000L)
  }

  test("a lost primary checkpoint falls back to the one-behind .bak buffer") {
    val f = cpFile("durable")
    val wm = new OffsetWatermark(fs, f, start = 0L)
    wm.complete(0L, 1000L) // primary <- 1000 (no bak yet)
    wm.complete(1000L, 2000L) // bak <- 1000, primary <- 2000
    // Simulate a crash that lost the primary mid-write; the .bak still holds the prior value.
    assert(fs.delete(f, /*recursive=*/ false))
    val recovered = new OffsetWatermark(fs, f, start = 0L)
    assert(recovered.committedOffset == 1000L, "must recover the one-behind offset from .bak")
  }

  test("start offset is ignored once a checkpoint exists") {
    val f = cpFile("startignored")
    new OffsetWatermark(fs, f, start = 0L).complete(0L, 500L)
    // A later run configured with a different start must still resume from the checkpoint.
    val next = new OffsetWatermark(fs, f, start = 99999L)
    assert(next.committedOffset == 500L)
  }
}
