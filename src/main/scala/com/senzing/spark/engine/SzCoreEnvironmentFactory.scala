package com.senzing.spark.engine

import com.senzing.sdk.SzEnvironment
import com.senzing.sdk.core.SzCoreEnvironment

/**
 * The only sz_spark class that names the SDK implementation. Keep it that way: it is defined by
 * whichever loader defines `com.senzing.sdk.core` (see [[SzEnvironmentFactory.nativeAware]]), so
 * anything else referencing `com.senzing.sdk.core` directly would bind the SDK to the wrong loader.
 */
final class SzCoreEnvironmentFactory extends SzEnvironmentFactory {

  def build(instanceName: String, settings: String): SzEnvironment =
    SzCoreEnvironment
      .newBuilder()
      .instanceName(instanceName)
      .settings(settings)
      .verboseLogging(false)
      .build()
}
