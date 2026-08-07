package com.senzing.spark.glue

import java.nio.charset.StandardCharsets
import java.util.concurrent.{CompletableFuture, Future}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.collection.mutable.ArrayBuffer

import org.apache.kafka.clients.producer.{Producer, ProducerRecord, RecordMetadata}
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyLong, anyString}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.funsuite.AnyFunSuite

import com.rabbitmq.client.{Channel, Envelope, GetResponse}

/**
 * The RabbitMQ→Kafka bridge's write-ahead invariant under a MOCK channel + MOCK producer (no
 * broker): every delivery must be PRODUCED to Kafka (and its send acknowledged) STRICTLY BEFORE any
 * RabbitMQ `basicAck` — an ack-before-produce would silently drop records. Plus the pure throttle
 * boundary.
 */
final class MqToKafkaSpec extends AnyFunSuite {

  private def resp(tag: Long, recordId: String): GetResponse = {
    val body =
      s"""{"DATA_SOURCE":"TESTSRC","RECORD_ID":"$recordId"}""".getBytes(StandardCharsets.UTF_8)
    new GetResponse(new Envelope(tag, false, "", "q"), null, body, 0)
  }

  test("shouldThrottle fires at (and above) the maxLag boundary, not below") {
    assert(!MqToKafka.shouldThrottle(latest = 4999999L, committed = 0L, maxLag = 5000000L))
    assert(MqToKafka.shouldThrottle(latest = 5000000L, committed = 0L, maxLag = 5000000L))
    assert(!MqToKafka.shouldThrottle(latest = 6000000L, committed = 1500000L, maxLag = 5000000L))
    assert(MqToKafka.shouldThrottle(latest = 6500000L, committed = 1000000L, maxLag = 5000000L))
  }

  test("produces every delivery to Kafka BEFORE acking any (write-ahead), then acks all tags") {
    val channel = mock(classOf[Channel])
    when(channel.basicGet(anyString(), anyBoolean()))
      .thenReturn(resp(1L, "A"), resp(2L, "B"), resp(3L, "C"), null)

    val producer = mock(classOf[Producer[Array[Byte], Array[Byte]]])
    val sent = new AtomicInteger(0)
    val values = ArrayBuffer.empty[String]
    when(producer.send(any[ProducerRecord[Array[Byte], Array[Byte]]]()))
      .thenAnswer(new Answer[Future[RecordMetadata]] {
        override def answer(inv: InvocationOnMock): Future[RecordMetadata] = {
          val rec = inv.getArgument(0).asInstanceOf[ProducerRecord[Array[Byte], Array[Byte]]]
          values += new String(rec.value(), StandardCharsets.UTF_8)
          sent.incrementAndGet()
          CompletableFuture.completedFuture(null.asInstanceOf[RecordMetadata])
        }
      })

    // At the instant of ANY ack, all three sends must already have happened.
    val ackViolation = new AtomicBoolean(false)
    doAnswer(new Answer[Object] {
      override def answer(inv: InvocationOnMock): Object = {
        if (sent.get() != 3) ackViolation.set(true)
        null
      }
    }).when(channel).basicAck(anyLong(), anyBoolean())

    val n = MqToKafka.drainAndProduce(channel, "q", producer, "sz-records", 5000)

    assert(n == 3, "all three buffered deliveries produced+acked")
    assert(!ackViolation.get(), "ack must never precede produce (write-ahead invariant)")
    verify(producer, times(3)).send(any[ProducerRecord[Array[Byte], Array[Byte]]]())
    verify(producer, times(1)).flush()
    verify(channel, times(3)).basicAck(anyLong(), anyBoolean())
    assert(
      values.toSeq.sorted ==
        Seq("A", "B", "C").map(id => s"""{"DATA_SOURCE":"TESTSRC","RECORD_ID":"$id"}"""),
      "the raw RabbitMQ body is the Kafka value, unchanged"
    )
  }

  test("an immediately-empty queue produces nothing and acks nothing") {
    val channel = mock(classOf[Channel])
    when(channel.basicGet(anyString(), anyBoolean())).thenReturn(null)
    val producer = mock(classOf[Producer[Array[Byte], Array[Byte]]])

    val n = MqToKafka.drainAndProduce(channel, "q", producer, "sz-records", 5000)

    assert(n == 0)
    verify(producer, never()).send(any[ProducerRecord[Array[Byte], Array[Byte]]]())
    verify(channel, never()).basicAck(anyLong(), anyBoolean())
  }
}
