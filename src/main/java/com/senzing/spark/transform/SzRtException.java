package com.senzing.spark.transform;

/**
 * A record-transform plugin failure. Thrown by {@link SzRtNative}: for {@code transform} it means the plugin rejected THIS record
 * (SZ_RT_ERROR, a per-record bad input); for {@code open} it means the plugin could not be loaded or created (systemic).
 */
public final class SzRtException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public SzRtException(String message) {
    super(message);
  }
}
