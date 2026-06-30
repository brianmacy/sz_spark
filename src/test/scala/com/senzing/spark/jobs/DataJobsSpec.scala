package com.senzing.spark.jobs

import java.nio.file.Files

import com.senzing.spark.model.{Op, StagingKind}
import com.senzing.spark.work._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Serializable fakes (captured by Spark closures must not reference the test instance). */
object DataJobsSpec {
  def affected(id: Long): InputRecord => String =
    _ => s"""{"AFFECTED_ENTITIES":[{"ENTITY_ID":$id}]}"""
  def searchResult: InputRecord => String =
    _ => """{"RESOLVED_ENTITIES":[]}"""
  def worker(op: WorkerOp, verb: InputRecord => String): RecordWorker =
    new RecordWorker(
      op,
      "run",
      verb,
      new Counters,
      new ProgressLogger("p", 0, () => 0L, _ => ()),
      sleep = _ => (),
      rnd = () => 0.5
    )
}

final class DataJobsSpec extends AnyFunSuite with BeforeAndAfterAll {
  import DataJobsSpec._

  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession
      .builder()
      .appName("sz-spark-datajobs")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def goodRows(
      op: WorkerOp,
      recs: Seq[InputRecord],
      verb: InputRecord => String
  ): Array[Row] = {
    val ss = spark
    import ss.implicits._
    val ds = recs.toDS()
    val staging = Files.createTempDirectory("staging").toFile
    staging.delete()
    SparkRecordOps.run(ss, ds, staging.getAbsolutePath, () => worker(op, verb)).good.collect()
  }

  test("add job tags affected-entity rows ADD") {
    val rows = goodRows(WorkerOp.Add, Seq(InputRecord("C", "1", "p")), affected(10L))
    assert(rows.length == 1 && rows.head.getAs[String]("op") == Op.Add)
    assert(rows.head.getAs[String]("kind") == StagingKind.Affected)
  }

  test("delete job tags affected-entity rows DELETE") {
    val rows = goodRows(WorkerOp.Delete, Seq(InputRecord("C", "1", "p")), affected(11L))
    assert(rows.length == 1 && rows.head.getAs[String]("op") == Op.Delete)
  }

  test("redo job tags affected-entity rows REDO (payload is the redo record)") {
    val rows = goodRows(WorkerOp.Redo, Seq(InputRecord("REDO", "", "<redo-json>")), affected(12L))
    assert(rows.length == 1 && rows.head.getAs[String]("op") == Op.Redo)
  }

  test("search job emits search-result rows") {
    val rows =
      goodRows(WorkerOp.Search, Seq(InputRecord("", "", """{"NAME_FULL":"x"}""")), searchResult)
    assert(rows.length == 1 && rows.head.getAs[String]("kind") == StagingKind.Search)
    assert(rows.head.getAs[String]("requestJson") == """{"NAME_FULL":"x"}""")
  }
}
