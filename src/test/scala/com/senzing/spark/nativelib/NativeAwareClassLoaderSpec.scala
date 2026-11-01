package com.senzing.spark.nativelib

import java.io.File
import java.nio.file.Files

import org.scalatest.funsuite.AnyFunSuite

/** Loaded child-first by the spec below (a real class from the test class path, not a stub). */
final class ChildFirstFixture

final class NativeAwareClassLoaderSpec extends AnyFunSuite {

  private val fixture = classOf[ChildFirstFixture].getName
  private val testClasses = classOf[ChildFirstFixture].getProtectionDomain.getCodeSource.getLocation

  private def libDirWith(files: String*): File = {
    val dir = Files.createTempDirectory("nacl").toFile
    files.foreach(n => new File(dir, n).createNewFile())
    dir
  }

  private def loader(libDir: File) =
    new NativeAwareClassLoader(Seq(testClasses), getClass.getClassLoader, libDir, Seq(fixture))

  private def findLibrary(l: ClassLoader, name: String): String = {
    val m = classOf[ClassLoader].getDeclaredMethod("findLibrary", classOf[String])
    m.setAccessible(true)
    m.invoke(l, name).asInstanceOf[String]
  }

  test("libraryPath maps a JNI name to <libDir>/System.mapLibraryName(name) when the file exists") {
    val dir = libDirWith(System.mapLibraryName("Sz"))
    assert(
      NativeAwareClassLoader.libraryPath(dir, "Sz") ==
        Some(new File(dir, System.mapLibraryName("Sz")).getAbsolutePath)
    )
    assert(NativeAwareClassLoader.libraryPath(dir, "Absent").isEmpty)
  }

  test("findLibrary answers from libDir, else falls back to the default (null)") {
    val dir = libDirWith(System.mapLibraryName("Sz"))
    val l = loader(dir)
    try {
      assert(findLibrary(l, "Sz") == new File(dir, System.mapLibraryName("Sz")).getAbsolutePath)
      assert(findLibrary(l, "Absent") == null)
    } finally l.close()
  }

  test("child-first names are defined by this loader; everything else comes from the parent") {
    val l = loader(libDirWith())
    try {
      val child = l.loadClass(fixture)
      assert(child.getClassLoader eq l)
      assert(child ne classOf[ChildFirstFixture])
      assert(l.loadClass(fixture) eq child) // defined once
      assert(l.loadClass(classOf[NativePaths].getName) eq classOf[NativePaths])
    } finally l.close()
  }

  test("a child-first name missing from the urls fails loudly (no silent parent fallback)") {
    val l =
      new NativeAwareClassLoader(Seq.empty, getClass.getClassLoader, libDirWith(), Seq(fixture))
    try assertThrows[ClassNotFoundException](l.loadClass(fixture))
    finally l.close()
  }
}
