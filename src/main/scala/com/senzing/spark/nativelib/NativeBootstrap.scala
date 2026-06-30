package com.senzing.spark.nativelib

import java.io.{File, InputStream, RandomAccessFile}
import java.nio.file.{Files, StandardCopyOption}
import java.security.MessageDigest
import java.util.jar.JarFile

import scala.jdk.CollectionConverters._

/**
 * Extracts the bundled native payload from the FAT jar to a node-local directory, exactly once per
 * node. Concurrency is guarded by BOTH a JVM-wide monitor (serializes threads in this executor) AND
 * an OS file lock (serializes across processes/executors on the node); a `.ready` sentinel lets
 * late arrivals skip. Publishing is atomic (extract into a temp dir, then rename).
 *
 * The actual copy is injected (`extract`) so the lock/atomicity/subtree logic is unit-testable
 * without real binaries; the live jar-scan + `System.load` path is exercised by FatJarIT.
 */
object NativeBootstrap {

  final val MarkerLib = "libSz.so"
  final val ReadySentinel = ".ready"
  final val DefaultMinFreeBytes: Long = 1L * 1024 * 1024 * 1024 // 1 GiB

  private val jvmLock = new Object

  /** Map `os.arch` to the staged native subdir segment. */
  def archSegment(osArch: String): String = osArch.toLowerCase match {
    case "amd64" | "x86_64" => "x86_64"
    case "aarch64" | "arm64" => "aarch64"
    case other => throw new RuntimeException(s"Unsupported os.arch for native extraction: $other")
  }

  def extractDirName(jarSha: String): String = s"sz-spark-$jarSha"

  /**
   * Ensure the payload is extracted under `baseDir`, returning the ready directory.
   *
   * @param extract
   *   performs the copy into a fresh temp dir (injected for tests / jar-scan at runtime)
   * @param usableBytes
   *   free-space probe (injectable)
   * @param minFreeBytes
   *   fail loudly below this many free bytes
   */
  def ensureExtracted(
      baseDir: File,
      jarSha: String,
      extract: File => Unit,
      usableBytes: File => Long = _.getUsableSpace,
      minFreeBytes: Long = DefaultMinFreeBytes
  ): File = jvmLock.synchronized {
    val dest = new File(baseDir, extractDirName(jarSha))
    val ready = new File(dest, ReadySentinel)
    if (ready.isFile) return dest

    baseDir.mkdirs()
    val free = usableBytes(baseDir)
    if (free < minFreeBytes)
      throw new RuntimeException(
        s"Insufficient free space in $baseDir: $free bytes < required $minFreeBytes"
      )

    val lockFile = new File(baseDir, s".${extractDirName(jarSha)}.lock")
    val raf = new RandomAccessFile(lockFile, "rw")
    try {
      val lock = raf.getChannel.lock() // exclusive, cross-process; JVM monitor already held
      try {
        if (!ready.isFile) {
          val tmp = new File(baseDir, s"${extractDirName(jarSha)}.tmp")
          deleteRecursively(tmp)
          tmp.mkdirs()
          extract(tmp) // may throw → no .ready written, dest never published
          if (dest.exists()) deleteRecursively(dest)
          try Files.move(tmp.toPath, dest.toPath, StandardCopyOption.ATOMIC_MOVE)
          catch { case _: Exception => Files.move(tmp.toPath, dest.toPath) }
          new File(dest, ReadySentinel).createNewFile()
        }
      } finally lock.release()
    } finally raf.close()
    dest
  }

  /**
   * True only when running from an actual FAT jar: the marker native resource is present AND this
   * code is loaded from a jar FILE (not a classes directory). The directory check is essential —
   * `stageNatives` puts the marker under `src/main/resources/`, so in dev/test the resource exists
   * but the code source is a directory; without this check we'd wrongly take the extract path and
   * try to hash a directory.
   */
  def isFatJar(
      loader: ClassLoader = getClass.getClassLoader,
      arch: String = archSegment(System.getProperty("os.arch", ""))
  ): Boolean =
    loader.getResource(s"native/linux-$arch/lib/$MarkerLib") != null && codeSourceIsJarFile

  private def codeSourceIsJarFile: Boolean =
    try new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI).isFile
    catch { case _: Throwable => false }

  /** SHA-256 (hex) of a file, streamed so a multi-hundred-MB jar never lands in heap. */
  def sha256(file: File): String = {
    val md = MessageDigest.getInstance("SHA-256")
    val buf = new Array[Byte](1 << 16)
    val in = Files.newInputStream(file.toPath)
    try {
      var n = in.read(buf)
      while (n != -1) { md.update(buf, 0, n); n = in.read(buf) }
    } finally in.close()
    md.digest().map("%02x".format(_)).mkString
  }

  /**
   * Runtime extractor: copy every `native/linux-<arch>/…` entry from `jar` into `destRoot`,
   * preserving subtree structure and marking `.so` executable. Used as the `extract` fn at runtime.
   */
  def extractJarResources(jar: File, arch: String, destRoot: File): Unit = {
    val prefix = s"native/linux-$arch/"
    val jf = new JarFile(jar)
    try {
      jf.entries().asScala.foreach { e =>
        if (!e.isDirectory && e.getName.startsWith(prefix)) {
          val rel = e.getName.substring(prefix.length)
          val target = new File(destRoot, rel)
          Option(target.getParentFile).foreach(_.mkdirs())
          val in: InputStream = jf.getInputStream(e)
          try Files.copy(in, target.toPath, StandardCopyOption.REPLACE_EXISTING)
          finally in.close()
          if (rel.endsWith(".so") || rel.endsWith(".dylib")) target.setExecutable(true)
        }
      }
    } finally jf.close()
  }

  def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteRecursively))
    f.delete()
  }
}
