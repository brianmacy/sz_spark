package com.senzing.spark.jobs

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import com.senzing.sdk.SzBadInputException
import com.senzing.spark.work._
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Spark closures must capture only this top-level object (a serializable singleton) — never the
 * ScalaTest instance — so the verb, worker factory, and counters live here.
 */
object SparkRecordOpsSpec {
  val verbCalls = new AtomicInteger(0)
  val acquires = new AtomicInteger(0)
  val releases = new AtomicInteger(0)

  def verb(r: InputRecord): String = {
    verbCalls.incrementAndGet()
    if (r.recordId == "bad") throw new SzBadInputException("bad row")
    s"""{"AFFECTED_ENTITIES":[{"ENTITY_ID":${r.recordId.toLong}}]}"""
  }

  def mkWorker(): RecordWorker =
    new RecordWorker(
      op = WorkerOp.Add,
      runId = "run-1",
      verb = verb,
      counters = new Counters,
      progress = new ProgressLogger("p", 0, () => 0L, _ => ()),
      sleep = _ => (),
      rnd = () => 0.5
    )

  def acquire(): Unit = { acquires.incrementAndGet(); () }
  def release(): Unit = { releases.incrementAndGet(); () }
}

final class SparkRecordOpsSpec extends AnyFunSuite with BeforeAndAfterAll {
  import SparkRecordOpsSpec._

  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession
      .builder()
      .appName("sz-spark-test")
      .master("local[2]")
      .config("spark.speculation", "false")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

  override def afterAll(): Unit = if (spark != null) spark.stop()

  test(
    "engine runs exactly once per record; good/error split; balanced acquire/release; no speculation"
  ) {
    verbCalls.set(0); acquires.set(0); releases.set(0)
    val ss = spark
    import ss.implicits._

    val recs = Seq(
      InputRecord("C", "1", "p"),
      InputRecord("C", "2", "p"),
      InputRecord("C", "3", "p"),
      InputRecord("C", "bad", "p")
    )
    val ds = recs.toDS()
    val staging = Files.createTempDirectory("staging").toFile
    staging.delete()

    val res = SparkRecordOps.run(ss, ds, staging.getAbsolutePath, mkWorker _, acquire _, release _)

    val good = res.good.collect()
    val errs = res.errors.collect()

    assert(verbCalls.get() == 4, s"engine must run once per record, was ${verbCalls.get()}")
    assert(good.length == 3, "three resolved entities")
    assert(errs.length == 1, "one bad-input error row")
    assert(acquires.get() > 0 && acquires.get() == releases.get(), "acquire/release must balance")
    assert(spark.conf.get("spark.speculation") == "false")
  }
}
