package com.senzing.spark.engine

import java.io.File
import java.net.URLClassLoader
import java.nio.file.Files

import com.senzing.sdk.SzEnvironment
import com.senzing.spark.nativelib.NativeAwareClassLoader
import org.scalatest.funsuite.AnyFunSuite

/**
 * The loader wiring of the SDK seam, with the real SDK jar (no natives needed: nothing here builds
 * an environment, so no `Native*Jni` class is initialised).
 */
final class SzEnvironmentFactorySpec extends AnyFunSuite {

  private val core = "com.senzing.sdk.core.SzCoreEnvironment"

  test("nativeAware defines the SDK implementation + our impl in ONE native-aware loader") {
    val libDir = Files.createTempDirectory("szef").toFile
    val factory = SzEnvironmentFactory.nativeAware(libDir)
    val loader = factory.getClass.getClassLoader
    assert(factory.getClass.getName == SzEnvironmentFactory.ImplClass)
    loader match {
      case l: NativeAwareClassLoader =>
        assert(l.libDir == libDir)
        assert(l.loadClass(core).getClassLoader eq l)
        assert(l.loadClass("com.senzing.sdk.core.NativeEngineJni").getClassLoader eq l)
        // The SDK interfaces stay shared, so callers use the environment through them unchanged.
        assert(l.loadClass(classOf[SzEnvironment].getName) eq classOf[SzEnvironment])
        assert(l.loadClass(classOf[SzEnvironmentFactory].getName) eq classOf[SzEnvironmentFactory])
      case other => fail(s"expected a NativeAwareClassLoader, got $other")
    }
  }

  test("direct builds through the normal class path") {
    assert(SzEnvironmentFactory.direct().getClass.getClassLoader eq getClass.getClassLoader)
  }

  test("codeSource resolves a class to its jar file or classes directory") {
    val jar = SzEnvironmentFactory.codeSource(getClass.getClassLoader, core)
    assert(jar.getProtocol == "file" && jar.getPath.endsWith(".jar"), jar)
    val dir =
      SzEnvironmentFactory.codeSource(getClass.getClassLoader, SzEnvironmentFactory.ImplClass)
    assert(new File(dir.toURI).isDirectory, dir)
    val probe = new URLClassLoader(Array(dir), null)
    try
      assert(probe.getResource(SzEnvironmentFactory.ImplClass.replace('.', '/') + ".class") != null)
    finally probe.close()
  }

  test("codeSource fails loudly for a class that is not on the class path") {
    assertThrows[IllegalStateException](
      SzEnvironmentFactory.codeSource(getClass.getClassLoader, "com.example.Absent")
    )
  }
}
