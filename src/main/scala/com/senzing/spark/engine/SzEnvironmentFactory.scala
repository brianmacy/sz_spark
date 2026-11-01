package com.senzing.spark.engine

import java.io.File
import java.net.{URI, URL}

import com.senzing.sdk.SzEnvironment
import com.senzing.spark.nativelib.NativeAwareClassLoader

/**
 * The narrow seam between sz_spark and the Senzing SDK implementation (`com.senzing.sdk.core`).
 * Callers only see the SDK interfaces (`SzEnvironment`, `SzEngine`, `SzException`, ...), which stay
 * on the normal class path; the one class that names `SzCoreEnvironment`
 * ([[SzCoreEnvironmentFactory]]) can therefore be defined, together with `com.senzing.sdk.core`, by
 * a [[NativeAwareClassLoader]].
 */
trait SzEnvironmentFactory {

  /**
   * Build the (single, per-JVM) environment. Callers enforce the one-instance rule
   * ([[SzEnvGuard]]).
   */
  def build(instanceName: String, settings: String): SzEnvironment
}

object SzEnvironmentFactory {

  final val ImplClass = "com.senzing.spark.engine.SzCoreEnvironmentFactory"
  private val CoreProbe = "com.senzing.sdk.core.SzCoreEnvironment"

  /** Defined child-first by the native-aware loader: the SDK implementation package + our impl. */
  final val ChildFirst: Seq[String] = Seq("com.senzing.sdk.core.", ImplClass)

  /**
   * The SDK on the normal class path: its `System.loadLibrary("Sz")` resolves through
   * `java.library.path`, i.e. a launch-time `LD_LIBRARY_PATH` (or an installed dist on it).
   */
  def direct(): SzEnvironmentFactory = new SzCoreEnvironmentFactory

  /**
   * The SDK defined by a [[NativeAwareClassLoader]] whose `findLibrary` answers from `libDir`, so
   * `libSz.so` is found there with no `LD_LIBRARY_PATH`. Create at most ONE per JVM (JNI binds a
   * library to a single class loader) and do not mix with [[direct]] in the same JVM;
   * [[SzEngineProvider]] calls it once, when it builds the engine. Do not `System.load` libSz.so
   * yourself first — that would bind it to the wrong loader.
   */
  def nativeAware(
      libDir: File,
      parent: ClassLoader = getClass.getClassLoader
  ): SzEnvironmentFactory = {
    val urls = Seq(codeSource(parent, CoreProbe), codeSource(parent, ImplClass)).distinct
    val loader = new NativeAwareClassLoader(urls, parent, libDir, ChildFirst)
    loader
      .loadClass(ImplClass)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[SzEnvironmentFactory]
  }

  /** The class-path root (a jar file or a classes directory) that holds `className` in `loader`. */
  private[engine] def codeSource(loader: ClassLoader, className: String): URL = {
    val resource = className.replace('.', '/') + ".class"
    val url = Option(loader.getResource(resource))
      .getOrElse(throw new IllegalStateException(s"$className is not on the class path"))
    val s = url.toExternalForm
    url.getProtocol match {
      case "jar" =>
        val inner = s.stripPrefix("jar:") // jar:file:/x.jar!/a/B.class
        URI.create(inner.substring(0, inner.lastIndexOf("!/"))).toURL
      case "file" => URI.create(s.stripSuffix(resource)).toURL
      case other =>
        throw new IllegalStateException(s"unsupported code source for $className: $other ($s)")
    }
  }
}
