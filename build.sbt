ThisBuild / organization := "com.senzing"
ThisBuild / version := "0.2.1"
// Spark 4.0 is built for Scala 2.13. Pin to the exact target Databricks runtime's 2.13.x.
ThisBuild / scalaVersion := "2.13.16"

// Anchor to the Databricks LTS: DBR 17.3 LTS runs Apache Spark 4.0.0 (supported to Oct 2028), so the
// jar is built against Spark 4.0.x (Provided), Scala 2.13. The on-prem rig runs a matching 4.0.x dist
// (NOT the staged 4.1.1). Delta (entity-mart workstream) pins delta-spark_4.0_2.13 (Delta 4.x supports
// Spark 4.0.1). DBR 18.2 / Spark 4.1 is latest-not-LTS; revisit only if we move off the LTS.
val sparkVersion = "4.0.1"
// Keep Jackson aligned to Spark's and `Provided`-adjacent (bundled copy used by non-Spark paths).
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
      // Kafka source (glue.KafkaSource, Step 2). Provided like spark-sql — supply at launch with
      // `--packages org.apache.spark:spark-sql-kafka-0-10_2.13:<sparkVersion>` (present on Databricks).
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % Provided,
      // Kafka producer/consumer clients: the on-prem producer (glue.FileToKafka), the driver-side
      // endOffsets metadata in glue.KafkaSource, and the glue.KafkaLag monitor. BUNDLED (not
      // Provided) so the standalone plain-JVM tools (KafkaLag) run with just the FAT jar; pinned to
      // Spark 4.0.1's kafka-clients (3.9.1) so the bundled copy is byte-identical to the cluster's on
      // the KafkaSource read path — no conflict.
      "org.apache.kafka" % "kafka-clients" % "3.9.1",
      // Delta source (glue.DeltaSource, Step 2c) — read a Delta table's change feed as a watermark
      // source. Provided: present on Databricks, or add via `--packages io.delta:delta-spark_2.13:4.0.0`.
      "io.delta" %% "delta-spark" % "4.0.0" % Provided,
      // WITH_INFO / search JSON parsing + engine-config rewrite. BUNDLED (not provided): these run on
      // the standalone engine path too (InitJob / SelfCheck, no Spark). Pinned to Spark 4.0's Jackson
      // so the bundled copy matches the cluster's — no version conflict.
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      // RabbitMQ ingest glue (MqToParquet) — Apache-2.0. Bundled (cluster does not ship it).
      "com.rabbitmq" % "amqp-client" % "5.21.0",
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
