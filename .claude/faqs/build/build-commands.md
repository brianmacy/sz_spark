# Build, test, and run commands

## Stack

Scala + sbt targeting **Spark 4.0 / Scala 2.13** (Spark 4 dropped 2.12; pin `scalaVersion` to the exact
Databricks runtime), calling the official Senzing **Java** SDK (the SDK already wraps the native engine
via JNI, so no custom FFI layer is needed). Spark dependencies are `% "provided"` — Spark is on the
cluster; the Senzing SDK jar (an **unmanaged** local dependency, `/opt/senzing/er/sdk/java/sz-sdk.jar`,
not a Maven coordinate) and native libs are bundled.

## Common sbt commands

```bash
sbt compile
sbt test
sbt "testOnly *FooSpec"                       # single test class
sbt "testOnly *FooSpec -- -z \"substring\""   # single test by name (ScalaTest -z)
sbt scalafmtAll                               # format
sbt stageNatives                              # stage Senzing libs/data/resources/config from a local
                                              # licensed dist into src/main/resources/native/ (gitignored)
sbt -J-Xmx8g assembly                         # FAT/uber jar (sbt-assembly; big heap for native payload)
```

## Why does assembly need a big heap?

The bundled native payload (libs + data + resources) is large. sbt-assembly reads files into heap for
merge/shade, so run it with a raised heap, e.g.:

```bash
sbt -J-Xmx8g assembly
```

## What produces the FAT jar?

`sbt assembly` (sbt-assembly), but only after the native resources have been staged into
`src/main/resources/native/linux-<arch>/` and `patchelf`'d. See the build/fat-jar-and-native-
extraction FAQ for the full sequence.
