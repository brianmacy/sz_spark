package com.senzing.spark.transform

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

import com.senzing.spark.engine.SzEngineProvider

/** A load-record rewrite applied just before `addRecord`: `None` = leave the record unchanged. */
trait RecordTransform extends AutoCloseable {
  def apply(record: String): Option[String]
}

/**
 * The plugin rejected this record (SZ_RT_ERROR); classified as BadInput and dead-lettered by the
 * worker.
 */
final class RecordTransformException(message: String) extends RuntimeException(message)

/**
 * A record-transform plugin (C ABI v1, the same libraries
 * `sz_queue_combined_consumer --record-transform-plugin` loads) behind the JNI shim. One instance
 * per executor JVM; `apply` is called concurrently from every task thread, which the ABI permits on
 * one handle.
 */
final class PluginTransform private (handle: Long) extends RecordTransform {
  override def apply(record: String): Option[String] =
    try Option(SzRtNative.transform(handle, record.getBytes(UTF_8))).map(new String(_, UTF_8))
    catch { case e: SzRtException => throw new RecordTransformException(e.getMessage) }

  override def close(): Unit = SzRtNative.close(handle)
}

object PluginTransform {
  final val ShimProperty =
    "sz.rt.jni.lib" // absolute path of libszrt_jni.so (dev/test; the FAT jar extracts it next to libSz.so)
  final val ShimName = "libszrt_jni.so"

  @volatile private var shimLoaded = false

  /**
   * Loads the JNI shim once per JVM: the property path if set, else the FAT-jar extraction dir
   * (created by the engine build).
   */
  private def loadShim(): Unit = if (!shimLoaded) synchronized {
    if (!shimLoaded) {
      val shim = sys.props
        .get(ShimProperty)
        .map(new File(_))
        .orElse(SzEngineProvider.nativeLibDir.map(new File(_, ShimName)))
      val file = shim.getOrElse(
        throw new IllegalStateException(
          s"cannot locate $ShimName: not running from the FAT jar and -D$ShimProperty is not set"
        )
      )
      if (!file.isFile)
        throw new IllegalStateException(s"record-transform JNI shim not found: $file")
      System.load(file.getAbsolutePath)
      shimLoaded = true
    }
  }

  /**
   * Throws (systemic: the task fails) when the shim or the plugin cannot be loaded; never falls
   * back to loading untransformed.
   */
  def open(pluginPath: String, config: String): PluginTransform = {
    val plugin = new File(pluginPath)
    if (!plugin.isFile)
      throw new IllegalStateException(
        s"record-transform plugin not found on this node: $pluginPath"
      )
    loadShim()
    new PluginTransform(SzRtNative.open(plugin.getAbsolutePath, config))
  }
}

/**
 * The per-JVM plugin singleton, configured from the executor environment with the same names as the
 * Rust consumer: `SENZING_RECORD_TRANSFORM_PLUGIN` (path of the .so; unset or empty = no hook) and
 * `SENZING_RECORD_TRANSFORM_CONFIG` (opaque string, default empty). Created on first use, destroyed
 * in the JVM shutdown hook.
 */
object RecordTransformProvider {
  final val PluginEnv = "SENZING_RECORD_TRANSFORM_PLUGIN"
  final val ConfigEnv = "SENZING_RECORD_TRANSFORM_CONFIG"

  private lazy val instance: Option[RecordTransform] =
    sys.env.get(PluginEnv).filter(_.trim.nonEmpty).map { path =>
      val t = PluginTransform.open(path, sys.env.getOrElse(ConfigEnv, ""))
      Runtime.getRuntime.addShutdownHook(
        new Thread(() => t.close(), "sz-record-transform-shutdown")
      )
      t
    }

  /**
   * The configured plugin, or None when no plugin is configured. A configured plugin that fails to
   * load throws.
   */
  def get(): Option[RecordTransform] = instance
}
