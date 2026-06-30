package com.senzing.spark.nativelib

import java.io.File
import java.nio.file.Files

import scala.collection.mutable.ListBuffer

import org.scalatest.funsuite.AnyFunSuite

final class NativeLibLoaderSpec extends AnyFunSuite {

  test("loadOrder is dependency-first with libSz always last") {
    assert(
      NativeLibLoader.loadOrder(None) ==
        Seq("libgcc_s.so.1", "libszzstd.so", "libszvec.so", "libSz.so")
    )
    assert(NativeLibLoader.loadOrder(None).last == "libSz.so")
  }

  test("the DB plugin loads immediately before libSz") {
    val order = NativeLibLoader.loadOrder(Some("libpostgresqlplugin.so"))
    assert(order.last == "libSz.so")
    assert(order(order.length - 2) == "libpostgresqlplugin.so")
  }

  test("loadAll loads only existing libs, in order, via the injected loader") {
    val dir = Files.createTempDirectory("nll").toFile
    // Only some siblings exist (libgcc_s is provided by the base image → absent here).
    Seq("libszzstd.so", "libszvec.so", "libSz.so").foreach(n => new File(dir, n).createNewFile())
    val calls = ListBuffer.empty[String]
    val loaded =
      NativeLibLoader.loadAll(
        dir,
        NativeLibLoader.loadOrder(None),
        p => calls += new File(p).getName
      )
    assert(calls.toList == List("libszzstd.so", "libszvec.so", "libSz.so"))
    assert(loaded.forall(_.startsWith(dir.getAbsolutePath)))
  }
}
