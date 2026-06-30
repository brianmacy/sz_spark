package com.senzing.spark

import org.scalatest.Tag

/**
 * Tag for `SZ_IT=1` integration tests that require a live Senzing engine + database. The default
 * `sbt test` (and CI) EXCLUDES this tag (see build.sbt `-l`); run them with `SZ_IT=1 sbt "testOnly
 * * -- -n com.senzing.spark.IntegrationTest"`.
 */
object IntegrationTest extends Tag("com.senzing.spark.IntegrationTest")
