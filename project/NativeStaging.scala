import sbt._

import scala.sys.process._

/**
 * Build-time staging of the Senzing native payload into the jar resources. Sourced from a LOCAL
 * licensed install ($SENZING_DIR); the staged tree is gitignored and the resulting FAT jar must never
 * be published (no SDK redistribution).
 *
 * Stages four DISTINCT trees, overlays user config, and patchelfs only bundled siblings (never
 * libSz.so, and without --force-rpath). The live dlopen-by-soname proof is FatJarIT (M16).
 */
object NativeStaging {

  // NEEDED non-system siblings a slim image may lack; patchelf'd to $ORIGIN if they ship without one.
  private val BundledSiblings = Seq("libgcc_s.so.1", "libssl.so.3", "libcrypto.so.3")

  def stage(senzingDir: File, arch: String, projectBase: File, log: String => Unit): Unit = {
    require(senzingDir.exists, s"SENZING_DIR not found: $senzingDir (need a local licensed install)")
    val target = projectBase / "src" / "main" / "resources" / "native" / s"linux-$arch"
    IO.delete(target)
    val lib = target / "lib"; val data = target / "data"
    val res = target / "resources"; val cfg = target / "config"
    Seq(lib, data, res, cfg).foreach(IO.createDirectory)

    // (2) Stock trees.
    IO.copyDirectory(senzingDir / "er" / "lib", lib)
    IO.copyDirectory(senzingDir / "data", data)
    IO.copyDirectory(senzingDir / "er" / "resources", res)
    // CONFIGPATH file set lives under resources/templates/ — staged into its own tree.
    val templates = senzingDir / "er" / "resources" / "templates"
    Seq("cfgVariant.json", "customGn.txt", "customOn.txt", "customSn.txt", "defaultGNRCP.config")
      .foreach { f =>
        val src = templates / f
        if (src.exists) IO.copyFile(src, cfg / f)
      }

    // (3) Overlay user config customizations (DevOps hook; ships empty).
    val overrides = projectBase / "config" / "overrides"
    Seq("data" -> data, "resources" -> res, "config" -> cfg).foreach { case (name, dst) =>
      val od = overrides / name
      if (od.exists) IO.copyDirectory(od, dst, overwrite = true)
    }

    // (3b) Minimize payload: strip symbols from every bundled .so. A no-op if the dist already ships
    // stripped (the current one does), but guards against an unstripped dist bloating the jar 10-25x.
    val hasStrip = (Seq("bash", "-c", "command -v strip").!) == 0
    if (hasStrip) {
      var saved = 0L
      Option(lib.listFiles()).getOrElse(Array.empty).filter(_.getName.endsWith(".so")).foreach { so =>
        val before = so.length()
        Seq("strip", "--strip-unneeded", so.getAbsolutePath).!
        saved += (before - so.length())
      }
      log(s"strip: removed ${saved / 1024 / 1024} MB of symbols from bundled .so libs")
    } else log("strip not found — skipping symbol strip (payload may be larger than necessary)")

    // (4) patchelf bundled siblings to $ORIGIN (NOT --force-rpath; NEVER libSz.so).
    val hasPatchelf = (Seq("bash", "-c", "command -v patchelf") .! ) == 0
    if (hasPatchelf) {
      BundledSiblings.foreach { so =>
        val f = lib / so
        if (f.exists) Seq("patchelf", "--set-rpath", "$ORIGIN", f.getAbsolutePath).!
      }
      log("patchelf: set RPATH=$ORIGIN on bundled siblings (libSz.so left untouched)")
    } else log("patchelf not found — skipping RPATH patch (bundled siblings may not resolve at runtime)")

    val soCount = Option(lib.listFiles()).getOrElse(Array.empty).count(_.getName.endsWith(".so"))
    log(s"Staged native payload to $target ($soCount .so libs)")
  }

  /** Verify an assembled jar actually contains the native marker + a minimum number of .so entries. */
  def verifyJar(jar: File, arch: String, log: String => Unit): Unit = {
    require(jar.exists, s"assembly jar not found: $jar")
    val entries = Seq("jar", "tf", jar.getAbsolutePath).!!.linesIterator.toVector
    val prefix  = s"native/linux-$arch/"
    val marker  = s"${prefix}lib/libSz.so"
    require(entries.contains(marker), s"FAT jar missing native marker $marker")
    val soCount = entries.count(e => e.startsWith(prefix) && e.endsWith(".so"))
    require(soCount >= 3, s"FAT jar has too few native .so entries ($soCount) — staging failed")
    log(s"verified FAT jar: marker present, $soCount native .so entries")
  }
}
