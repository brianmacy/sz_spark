package com.senzing.spark.nativelib

import java.io.File

/**
 * Loads the Senzing native libraries in dependency order so dlopen-by-soname resolves.
 *
 * `libSz.so` directly NEEDs only system libs (RUNPATH=$ORIGIN), but it `dlopen`s `libszvec.so`,
 * `libszzstd.so`, and the DB plugin **by soname** — and `$ORIGIN`/RUNPATH does NOT apply to
 * dlopen-by-soname. So a single `System.load(libSz)` is insufficient. The fix is BOTH: (1)
 * `spark.executorEnv.LD_LIBRARY_PATH=<extract>/lib` set at executor LAUNCH (resolves
 * dlopen-by-soname; cannot be set after JVM start), and (2) `System.load` every sibling in
 * dependency order before `libSz` so each later dlopen returns an already-loaded handle (this
 * object).
 */
object NativeLibLoader {

  /**
   * Dependency-first soname order. `libSz.so` is ALWAYS last; the DB plugin (if any) precedes it.
   */
  def loadOrder(dbPluginSoName: Option[String]): Seq[String] =
    Seq("libgcc_s.so.1", "libszzstd.so", "libszvec.so") ++ dbPluginSoName.toSeq :+ "libSz.so"

  /**
   * Load (via the injected loader — `System.load` at runtime) each lib in `order` that exists in
   * `libDir`, in order. Missing libs are skipped (e.g. `libgcc_s` provided by the base image).
   * Returns the absolute paths actually loaded.
   */
  def loadAll(libDir: File, order: Seq[String], load: String => Unit): Seq[String] = {
    val loaded = order
      .map(name => new File(libDir, name))
      .filter(_.isFile)
      .map(_.getAbsolutePath)
    loaded.foreach(load)
    loaded
  }

  /** Runtime convenience: load the standard chain from `libDir` via `System.load`. */
  def loadFatJarLibs(libDir: File, dbPluginSoName: Option[String]): Seq[String] =
    loadAll(libDir, loadOrder(dbPluginSoName), System.load)
}
