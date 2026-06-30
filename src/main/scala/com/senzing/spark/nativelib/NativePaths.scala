package com.senzing.spark.nativelib

import java.io.File

/**
 * The four extracted Senzing trees. They are DISTINCT directories — the engine config's three
 * PIPELINE keys point at different ones (SUPPORTPATH→data, RESOURCEPATH→resources,
 * CONFIGPATH→config), and `libDir` is flat so `$ORIGIN` resolves siblings.
 */
final case class NativePaths(libDir: File, dataDir: File, resourcesDir: File, configDir: File)

object NativePaths {

  /** Standard subtree layout under an extraction root. */
  def under(root: File): NativePaths =
    NativePaths(
      libDir = new File(root, "lib"),
      dataDir = new File(root, "data"),
      resourcesDir = new File(root, "resources"),
      // CONFIGPATH file set (cfgVariant.json / customGn|On|Sn.txt / defaultGNRCP.config) ships under
      // the dist's resources/templates/ — staged into its own `config` tree by stageNatives (M9).
      configDir = new File(root, "config")
    )
}
