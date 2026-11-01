package com.senzing.spark.nativelib

import java.io.File
import java.net.{URL, URLClassLoader}

/**
 * A class loader that answers `findLibrary` from an explicit native lib dir, so a JNI class it
 * defines can `System.loadLibrary(name)` without that dir being on `java.library.path` (i.e.
 * without a launch-time `LD_LIBRARY_PATH`). JNI binds the library to the class loader of the class
 * calling `loadLibrary`; the JDK consults that loader's `findLibrary` first and only falls back to
 * `java.library.path` when it returns null. Public JDK API only — no JDK internals.
 *
 * Names starting with a `childFirst` prefix are defined by THIS loader from `urls` (never delegated
 * to the parent, so a missing class fails loudly instead of silently binding to the parent's copy);
 * every other name is delegated parent-first as usual. A JVM must use one such loader for a given
 * library: JNI refuses to load the same library into a second class loader.
 */
final class NativeAwareClassLoader(
    urls: Seq[URL],
    parent: ClassLoader,
    val libDir: File,
    childFirst: Seq[String]
) extends URLClassLoader(urls.toArray, parent) {

  override protected def loadClass(name: String, resolve: Boolean): Class[_] =
    getClassLoadingLock(name).synchronized {
      val c: Class[_] = Option[Class[_]](findLoadedClass(name)).getOrElse {
        if (childFirst.exists(name.startsWith)) findClass(name) else super.loadClass(name, false)
      }
      if (resolve) resolveClass(c)
      c
    }

  override protected def findLibrary(name: String): String =
    NativeAwareClassLoader.libraryPath(libDir, name).getOrElse(super.findLibrary(name))
}

object NativeAwareClassLoader {

  /**
   * `<libDir>/System.mapLibraryName(name)` when that file exists (e.g. "Sz" → libSz.so), else None.
   */
  def libraryPath(libDir: File, name: String): Option[String] = {
    val f = new File(libDir, System.mapLibraryName(name))
    if (f.isFile) Some(f.getAbsolutePath) else None
  }
}
