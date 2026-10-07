package com.senzing.spark.transform;

/**
 * JNI bridge to a record-transform plugin (C ABI version 1: sz_rt_abi_version / sz_rt_create / sz_rt_transform / sz_rt_free /
 * sz_rt_destroy; contract in sz_queue_combined_consumer crates/transform-abi). Implemented by native/szrt/szrt_jni.c.
 *
 * <p>Ownership: the handle returned by {@link #open} belongs to the caller until {@link #close}. Plugin-allocated buffers never leave
 * the native side (they are copied to a Java array and released with sz_rt_free). {@link #transform} may be called concurrently from
 * any number of threads on one handle (the plugin ABI requires that); {@link #close} must not race a transform.
 */
public final class SzRtNative {
  private SzRtNative() {}

  /**
   * dlopen the plugin, check its ABI version, create an instance.
   *
   * @param libPath absolute path of the plugin shared library
   * @param config opaque config passed to sz_rt_create (UTF-8; "" when none)
   * @return a non-zero handle
   * @throws SzRtException the library cannot be opened, lacks a symbol, has another ABI version, or create failed
   */
  public static native long open(String libPath, String config) throws SzRtException;

  /**
   * Transform one UTF-8 record.
   *
   * @return null when the plugin leaves the record unchanged (SZ_RT_UNCHANGED), else the replacement UTF-8 bytes
   * @throws SzRtException the plugin rejected the record (SZ_RT_ERROR); the message is the plugin's
   */
  public static native byte[] transform(long handle, byte[] record) throws SzRtException;

  /** sz_rt_destroy and dlclose-free release of the handle (the library stays mapped). */
  public static native void close(long handle);
}
