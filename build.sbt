ThisBuild / organization := "com.senzing"
ThisBuild / version := "0.1.0"
// Spark 4.0 is built for Scala 2.13. Pin to the exact target Databricks runtime's 2.13.x.
ThisBuild / scalaVersion := "2.13.16"

// Spark stays on the 4.0 series (provided — pin to the Databricks runtime); 4.1/4.2 are deliberate
// non-adoptions until the target cluster moves.
val sparkVersion = "4.0.2"
// Spark 4.0 ships Jackson 2.18.x; keep ours aligned and `Provided` (the cluster provides it).
val jacksonVersion = "2.18.2"

// Senzing install root holding the Java SDK jar + native libs. NOT on Maven Central.
// Defaults to /opt/senzing; override with SENZING_DIR for a relocated licensed install.
val senzingDir: File =
  file(sys.env.getOrElse("SENZING_DIR", "/opt/senzing"))
val senzingSdkJar: File = senzingDir / "er" / "sdk" / "java" / "sz-sdk.jar"

// Spark on Java 17+/21 requires these module opens to run (driver + local tests).
val sparkJavaOpens: Seq[String] = Seq(
  "java.base/java.lang",
  "java.base/java.lang.invoke",
  "java.base/java.lang.reflect",
  "java.base/java.io",
  "java.base/java.net",
  "java.base/java.nio",
  "java.base/java.util",
  "java.base/java.util.concurrent",
  "java.base/java.util.concurrent.atomic",
  "java.base/sun.nio.ch",
  "java.base/sun.nio.cs",
  "java.base/sun.security.action",
  "java.base/sun.util.calendar",
  "java.base/jdk.internal.ref",
  "java.base/jdk.internal.misc"
).map(p => s"--add-opens=$p=ALL-UNNAMED")

val nativeArch    = sys.env.getOrElse("SENZING_ARCH", "x86_64")
lazy val stageNatives = taskKey[Unit]("Stage Senzing native libs/data/resources/config into jar resources (local, gitignored)")
lazy val verifyAssembly = taskKey[Unit]("Verify the assembled FAT jar contains the native payload")

lazy val root = (project in file("."))
  .settings(
    name := "sz-spark",
    stageNatives := NativeStaging.stage(senzingDir, nativeArch, baseDirectory.value, sLog.value.info(_)),
    verifyAssembly := NativeStaging.verifyJar(
      baseDirectory.value / "target" / s"scala-2.13" / (assembly / assemblyJarName).value,
      nativeArch,
      sLog.value.info(_)
    ),
    libraryDependencies ++= Seq(
      // Spark is on the cluster — never bundled.
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql"  % sparkVersion % Provided,
      // WITH_INFO / search JSON parsing + engine-config rewrite. BUNDLED (not provided): these run on
      // the standalone engine path too (InitJob / SelfCheck, no Spark). Pinned to Spark 4.0's Jackson
      // so the bundled copy matches the cluster's — no version conflict.
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      // InitJob schema/admin runs standalone (not on the cluster) — bundle a JDBC driver.
      "org.postgresql" % "postgresql" % "42.7.7",
      "org.scalatest" %% "scalatest"   % "3.2.19" % Test, // 3.3.0 is still a snapshot
      "org.mockito"    % "mockito-core" % "5.18.0" % Test
    ),
    // The Senzing Java SDK jar ships with senzingsdk-runtime, not Maven Central.
    // Reference the locally installed licensed copy; it is bundled into the FAT jar by assembly.
    Compile / unmanagedJars += Attributed.blank(senzingSdkJar),
    Test / unmanagedJars += Attributed.blank(senzingSdkJar),
    // Spark needs a forked test JVM with the module opens above.
    Test / fork := true,
    Test / parallelExecution := false, // local SparkSessions must not run concurrently in one JVM
    Test / javaOptions ++= sparkJavaOpens,
    // Exclude integration specs (tagged) from the default `test`; run them with `-n` + SZ_IT=1.
    Test / testOptions += Tests.Argument("-l", "com.senzing.spark.IntegrationTest"),
    // FAT jar (full native staging wired in via project/StageNatives.scala at M9).
    assembly / assemblyJarName := "sz-spark-assembly.jar",
    assembly / mainClass := None,
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) =>
        val lower = xs.map(_.toLowerCase)
        if (lower.lastOption.exists(p => p.endsWith(".sf") || p.endsWith(".dsa") || p.endsWith(".rsa")))
          MergeStrategy.discard
        else if (lower.headOption.contains("services")) MergeStrategy.concat
        else MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case p if p.startsWith("native/") =>
        MergeStrategy.singleOrError // native payload must be unique + verbatim
      case _ => MergeStrategy.first
    }
  )
