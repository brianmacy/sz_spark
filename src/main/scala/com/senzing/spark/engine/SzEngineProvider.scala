package com.senzing.spark.engine

import java.io.File
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.jdk.CollectionConverters._

import com.senzing.sdk.{SzEngine, SzEnvironment}
import com.senzing.sdk.core.SzCoreEnvironment
import com.senzing.spark.nativelib._

/**
 * The per-executor-JVM Senzing engine singleton. Built ONCE per JVM (FAT-jar self-extraction +
 * ordered native load + `SzCoreEnvironment.build`), destroyed only at JVM shutdown via one ordered
 * hook. `acquire`/`release` maintain a liveness counter only. Verbs run under a read lock; config
 * reinit takes the write lock to quiesce in-flight verbs.
 *
 * The lifecycle logic lives in [[EngineLifecycle]] (unit-tested); this object wires the real native
 * + SDK build path, which is exercised by FatJarIT / LoadIT.
 */
object SzEngineProvider {

  final val InstanceName = "sz-spark-exec"

  private val rwl = new ReentrantReadWriteLock()
  @volatile private var extractRoot: Option[File] = None

  private val lifecycle =
    new EngineLifecycle(
      build = buildEnv,
      destroy = env =>
        try env.destroy()
        finally SzEnvGuard.markDestroyed(),
      cleanup = cleanupExtract
    )

  Runtime.getRuntime.addShutdownHook(new Thread(() => lifecycle.shutdown(), "sz-engine-shutdown"))

  private def envMap: Map[String, String] = System.getenv().asScala.toMap

  private def buildEnv(): SzEnvironment = {
    GlibcCheck.enforce()
    val base = EngineSettings.fromEnv(envMap)
    val settings =
      if (NativeBootstrap.isFatJar()) {
        val arch = NativeBootstrap.archSegment(System.getProperty("os.arch", ""))
        val jar = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI)
        val baseDir = new File(sys.env.getOrElse("SENZING_EXTRACT_DIR", "/var/tmp"))
        val dest = NativeBootstrap.ensureExtracted(
          baseDir,
          NativeBootstrap.sha256(jar),
          tmp => NativeBootstrap.extractJarResources(jar, arch, tmp)
        )
        extractRoot = Some(dest)
        val paths = NativePaths.under(dest)
        // DB plugin is dlopen'd via the launch-time LD_LIBRARY_PATH; siblings loaded in order here.
        NativeLibLoader.loadFatJarLibs(paths.libDir, dbPluginSoName = None)
        EngineSettings.rewritePaths(base, paths)
      } else base

    SzEnvGuard.markBuilt()
    SzCoreEnvironment
      .newBuilder()
      .instanceName(InstanceName)
      .settings(settings)
      .verboseLogging(false)
      .build()
  }

  private def cleanupExtract(): Unit =
    if (!sys.env.get("SENZING_KEEP_EXTRACTED").contains("true"))
      extractRoot.foreach(NativeBootstrap.deleteRecursively)

  /** Acquire the shared environment (builds once); pair with [[release]] in a finally. */
  def acquire(): SzEnvironment = lifecycle.acquire()
  def release(): Unit = lifecycle.release()

  /** The shared engine. */
  def engine(): SzEngine = acquire().getEngine()

  /** Run a verb under the read lock so a concurrent config reinit (write lock) quiesces it. */
  def withReadLock[T](body: => T): T = {
    rwl.readLock().lock()
    try body
    finally rwl.readLock().unlock()
  }

  /** Run a reinit under the write lock, draining in-flight verbs first. */
  def withWriteLock[T](body: => T): T = {
    rwl.writeLock().lock()
    try body
    finally rwl.writeLock().unlock()
  }
}
