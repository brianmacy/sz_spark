package com.senzing.spark.glue

import java.io.{File, FilenameFilter}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.sql.SparkSession
import org.mockito.ArgumentMatchers.{anyBoolean, anyLong, anyString}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import com.rabbitmq.client.{Channel, Envelope, GetResponse}
import com.senzing.spark.work.InputRecord

/**
 * The write-ahead invariant under a MOCK RabbitMQ channel (no broker): the shard must be persisted
 * and the `.tmp-` dir renamed to `part-<uuid>.parquet` STRICTLY BEFORE any `basicAck` — an
 * ack-before-persist would silently drop records.
 */
final class MqToParquetSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession
      .builder()
      .appName("sz-mq-to-parquet-test")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def resp(tag: Long, recordId: String): GetResponse = {
    val body =
      s"""{"DATA_SOURCE":"TESTSRC","RECORD_ID":"$recordId","NAME_FULL":"x"}"""
        .getBytes(StandardCharsets.UTF_8)
    new GetResponse(new Envelope(tag, false, "", "q"), null, body, 0)
  }

  private def matching(inbox: File, pred: String => Boolean): Array[File] =
    Option(inbox.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = pred(name)
    })).getOrElse(Array.empty)

  private def partFiles(inbox: File): Array[File] =
    matching(inbox, n => n.startsWith("part-") && n.endsWith(".parquet"))
  private def tmpFiles(inbox: File): Array[File] = matching(inbox, _.startsWith(".tmp-"))

  test(
    "persists + renames the shard BEFORE acking any delivery (write-ahead), then acks all tags"
  ) {
    val inbox = Files.createTempDirectory("inbox").toFile
    val channel = mock(classOf[Channel])
    when(channel.basicGet(anyString(), anyBoolean()))
      .thenReturn(resp(1L, "A"), resp(2L, "B"), resp(3L, "C"), null)

    // At the instant of ANY ack, the renamed shard must exist and no tmp may remain.
    val ackViolation = new AtomicBoolean(false)
    doAnswer(new Answer[Object] {
      override def answer(invocation: InvocationOnMock): Object = {
        if (partFiles(inbox).isEmpty || tmpFiles(inbox).nonEmpty) ackViolation.set(true)
        null
      }
    }).when(channel).basicAck(anyLong(), anyBoolean())

    val n =
      MqToParquet.drainAndPersistShard(channel, "q", inbox.getAbsolutePath, 5000, spark)

    assert(n == 3, "all three buffered deliveries persisted+acked")
    assert(!ackViolation.get(), "ack must never precede persist+rename (write-ahead invariant)")
    assert(partFiles(inbox).nonEmpty, "a part-<uuid>.parquet shard was produced")
    assert(tmpFiles(inbox).isEmpty, "no .tmp- shard left behind after the atomic rename")
    verify(channel, times(3)).basicAck(anyLong(), anyBoolean())

    val ss = spark
    import ss.implicits._
    val back = ss.read.parquet(inbox.getAbsolutePath).as[InputRecord].collect().sortBy(_.recordId)
    assert(back.map(_.recordId).toSeq == Seq("A", "B", "C"), "RECORD_ID parsed from each body")
    assert(back.forall(_.dataSource == "TESTSRC"), "DATA_SOURCE parsed from each body")
  }

  test("an immediately-empty queue persists nothing and acks nothing") {
    val inbox = Files.createTempDirectory("inbox-empty").toFile
    val channel = mock(classOf[Channel])
    when(channel.basicGet(anyString(), anyBoolean())).thenReturn(null)

    val n =
      MqToParquet.drainAndPersistShard(channel, "q", inbox.getAbsolutePath, 5000, spark)

    assert(n == 0)
    assert(partFiles(inbox).isEmpty && tmpFiles(inbox).isEmpty)
    verify(channel, never()).basicAck(anyLong(), anyBoolean())
  }
}
