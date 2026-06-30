package com.senzing.spark.it

import java.io.File
import java.util.jar.JarFile

import scala.jdk.CollectionConverters._

import com.senzing.spark.IntegrationTest
import org.scalatest.funsuite.AnyFunSuite

/**
 * Verifies the assembled FAT jar actually bundles the native payload (marker lib + siblings + the
 * CONFIGPATH tree). Build it first locally: `sbt stageNatives -J-Xmx8g assembly`. The LIVE
 * dlopen-by-soname proof (load + a real verb on a slim image with no system Senzing, using launch
 * `LD_LIBRARY_PATH`) runs on the supported executor image — see docs/RUNBOOK.md.
 */
final class FatJarIT extends AnyFunSuite {

  private def enabled: Boolean = sys.env.get("SZ_IT").contains("1")

  test("FAT jar bundles the native marker, siblings, and CONFIGPATH tree", IntegrationTest) {
    assume(enabled, "requires SZ_IT=1")
    val jar = new File("target/scala-2.13/sz-spark-assembly.jar")
    assume(jar.exists, s"build the FAT jar first (sbt stageNatives assembly): $jar")

    val jf = new JarFile(jar)
    try {
      val names = jf.entries().asScala.map(_.getName).toVector
      val prefix = "native/linux-x86_64/"
      assert(names.contains(s"${prefix}lib/libSz.so"), "missing native marker libSz.so")
      assert(
        names.count(n => n.startsWith(s"${prefix}lib/") && n.endsWith(".so")) >= 3,
        "too few bundled native .so entries"
      )
      assert(names.exists(_.startsWith(s"${prefix}config/")), "missing CONFIGPATH tree")
    } finally jf.close()
  }
}
