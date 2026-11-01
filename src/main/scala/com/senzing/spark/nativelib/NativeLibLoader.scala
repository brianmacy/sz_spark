package com.senzing.spark.nativelib

import java.io.File

/**
 * Loads the Senzing native libraries in dependency order so dlopen-by-soname resolves.
 *
 * `libSz.so` `dlopen`s `libszvec.so`, `libszzstd.so`, and the DB plugin **by soname**, so each
 * helper is `System.load`ed by absolute path first and every later dlopen returns the
 * already-loaded handle. `libSz.so` itself is then loaded by the SDK's own
 * `System.loadLibrary("Sz")`, which finds it through the native-aware class loader's `findLibrary`
 * (no launch-time `LD_LIBRARY_PATH` needed); a launch-time `LD_LIBRARY_PATH=<extract>/lib` still
 * works as an alternative.
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

  /**
   * Runtime: `System.load` the helpers that `libSz.so` dlopens (everything in [[loadOrder]] EXCEPT
   * libSz.so). libSz.so itself is bound by the SDK through
   * [[com.senzing.spark.engine.SzEnvironmentFactory.nativeAware]] — JNI ties a library to ONE class
   * loader, so loading it here would bind it to the wrong one.
   */
  def loadHelpers(libDir: File, dbPluginSoName: Option[String]): Seq[String] =
    loadAll(libDir, helperOrder(dbPluginSoName), System.load)

  /** [[loadOrder]] without libSz.so. */
  def helperOrder(dbPluginSoName: Option[String]): Seq[String] =
    loadOrder(dbPluginSoName).filterNot(_ == NativeBootstrap.MarkerLib)
}
