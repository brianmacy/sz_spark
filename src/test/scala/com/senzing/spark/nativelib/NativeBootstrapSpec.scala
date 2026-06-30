package com.senzing.spark.nativelib

import java.io.File
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.funsuite.AnyFunSuite

final class NativeBootstrapSpec extends AnyFunSuite {

  private def tmpBase(): File = Files.createTempDirectory("nboot").toFile
  private def write(f: File, s: String): Unit = {
    Option(f.getParentFile).foreach(_.mkdirs())
    Files.writeString(f.toPath, s)
  }

  test("archSegment maps known arches and rejects others") {
    assert(NativeBootstrap.archSegment("amd64") == "x86_64")
    assert(NativeBootstrap.archSegment("x86_64") == "x86_64")
    assert(NativeBootstrap.archSegment("aarch64") == "aarch64")
    assert(NativeBootstrap.archSegment("arm64") == "aarch64")
    assertThrows[RuntimeException](NativeBootstrap.archSegment("sparc"))
  }

  test("sha256 streams a stable 64-hex digest") {
    val f = File.createTempFile("sha", ".bin")
    write(f, "senzing")
    val h = NativeBootstrap.sha256(f)
    assert(h.length == 64 && h.matches("[0-9a-f]+"))
    assert(h == NativeBootstrap.sha256(f)) // stable
  }

  test("exactly one of two racing threads extracts; sentinel published") {
    val base = tmpBase()
    val counter = new AtomicInteger(0)
    val extract: File => Unit = { tmp =>
      counter.incrementAndGet()
      write(new File(tmp, "lib/libSz.so"), "x")
    }
    val threads = (1 to 2).map(_ =>
      new Thread(() => NativeBootstrap.ensureExtracted(base, "race", extract, _ => Long.MaxValue))
    )
    threads.foreach(_.start()); threads.foreach(_.join())
    assert(counter.get() == 1, "extract must run exactly once per node")
    assert(new File(base, "sz-spark-race/.ready").isFile)
  }

  test("extraction preserves subtree structure and a flat lib dir") {
    val base = tmpBase()
    val dest = NativeBootstrap.ensureExtracted(
      base,
      "subtree",
      { tmp =>
        write(new File(tmp, "data/cfg/sub/x.txt"), "d")
        write(new File(tmp, "resources/templates/cfgVariant.json"), "{}")
        write(new File(tmp, "lib/libSz.so"), "elf")
      },
      _ => Long.MaxValue
    )
    assert(new File(dest, "data/cfg/sub/x.txt").isFile)
    assert(new File(dest, "resources/templates/cfgVariant.json").isFile)
    assert(new File(dest, "lib/libSz.so").isFile)
  }

  test("a failed extract publishes no sentinel and a retry succeeds (atomic publish)") {
    val base = tmpBase()
    assertThrows[RuntimeException](
      NativeBootstrap.ensureExtracted(
        base,
        "atomic",
        _ => throw new RuntimeException("boom"),
        _ => Long.MaxValue
      )
    )
    assert(!new File(base, "sz-spark-atomic/.ready").exists())
    val dest = NativeBootstrap.ensureExtracted(
      base,
      "atomic",
      tmp => write(new File(tmp, "lib/libSz.so"), "x"),
      _ => Long.MaxValue
    )
    assert(new File(dest, ".ready").isFile)
  }

  test("insufficient free space fails loudly before extracting") {
    val base = tmpBase()
    val ran = new AtomicInteger(0)
    assertThrows[RuntimeException](
      NativeBootstrap.ensureExtracted(
        base,
        "space",
        _ => ran.incrementAndGet(),
        usableBytes = _ => 10L,
        minFreeBytes = 1000L
      )
    )
    assert(ran.get() == 0)
  }
}
