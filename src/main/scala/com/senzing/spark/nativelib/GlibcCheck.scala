package com.senzing.spark.nativelib

import scala.sys.process._
import scala.util.Try

/**
 * Fails loudly at startup if the runtime glibc is below the floor the Senzing native libs require
 * (verified 2.34 from `readelf -V` on the dist). Pure check + parser; detection is injectable.
 */
object GlibcCheck {

  final val Floor: (Int, Int) = (2, 34)

  private val versionRe = """(\d+)\.(\d+)""".r

  /** Parse the first `MAJOR.MINOR` out of a version string (e.g. ldd/`getconf` output). */
  def parse(s: String): Option[(Int, Int)] =
    versionRe.findFirstMatchIn(s).map(m => (m.group(1).toInt, m.group(2).toInt))

  /** Pure: Right if `detected >= floor` (lexicographic), else Left with a message. */
  def check(detected: (Int, Int), floor: (Int, Int) = Floor): Either[String, Unit] = {
    val (dMaj, dMin) = detected
    val (fMaj, fMin) = floor
    val ok = dMaj > fMaj || (dMaj == fMaj && dMin >= fMin)
    if (ok) Right(())
    else Left(s"glibc $dMaj.$dMin is below the required floor $fMaj.$fMin")
  }

  /** Best-effort runtime detection via `getconf GNU_LIBC_VERSION` then `ldd --version`. */
  def detect(): String =
    Try(Seq("getconf", "GNU_LIBC_VERSION").!!).orElse(Try("ldd --version".!!)).getOrElse("").trim

  /** Enforce at startup; throw loudly if undetectable or below floor. */
  def enforce(detector: () => String = () => detect()): Unit = {
    val v = parse(detector()).getOrElse(
      throw new RuntimeException("Could not determine the runtime glibc version")
    )
    check(v).left.foreach(msg => throw new RuntimeException(s"Unsupported runtime: $msg"))
  }
}
