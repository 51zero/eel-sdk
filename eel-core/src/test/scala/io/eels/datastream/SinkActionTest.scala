package io.eels.datastream

import java.util.concurrent.{CountDownLatch, TimeUnit}

import io.eels.schema.{Field, StructType}
import io.eels.{Row, Sink, SinkWriter}
import org.scalatest.{Matchers, WordSpec}

import scala.util.Try

class SinkActionTest extends WordSpec with Matchers {

  "sink action" should {
    "handle errors when opening sinks" in {

      intercept[RuntimeException] {
        class ErrorSink extends Sink {
          override def open(schema: StructType): SinkWriter = sys.error("boom")
        }

        val ds = DataStream.fromValues(StructType(Field("name")), Seq(Seq("sam"), Seq("bob")))
        ds.to(new ErrorSink)
      }
    }
    "close stream when errored" in {

      var closed = false

      intercept[RuntimeException] {
        class ErrorSink extends Sink {
          override def open(schema: StructType): SinkWriter = new SinkWriter {
            override def close(): Unit = closed = true
            override def write(row: Row): Unit = sys.error("boom")
          }
        }

        val ds = DataStream.fromValues(StructType(Field("name")), Seq(Seq("sam"), Seq("bob")))
        ds.to(new ErrorSink)
      }

      closed shouldBe true
    }
    "return successfully when an error stops the writer writing" in {

      val schema = StructType(Field("a"))
      val ds = DataStream.fromIterator(schema, Iterator.continually(Row(schema, Seq("a"))))

      intercept[RuntimeException] {
        ds.to(new ErrorSink)
      }
    }
    "cancel upstream when a writer has an error" in {

      val latch = new CountDownLatch(1)
      val _schema = StructType(Field("a"))
      val ds = new DataStream {
        override def schema: StructType = _schema
        override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
          subscriber.subscribed(new Subscription {
            override def cancel(): Unit = latch.countDown()
          })
          Iterator.continually(Row(schema, Seq("a"))).takeWhile(_ => latch.getCount > 0).grouped(100).foreach(subscriber.next)
          subscriber.completed()
        }
      }
      intercept[RuntimeException] {
        ds.to(new ErrorSink, 8)
      }
      latch.await()
    }
    "return if all writers error and queue is backfilled" in {

      val _schema = StructType(Field("a"))
      val ds = new DataStream {
        override def schema: StructType = _schema
        override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
          subscriber.subscribed(Subscription.empty)
          Iterator.continually(Row(schema, Seq("a"))).take(100000).grouped(10).foreach(subscriber.next)
          subscriber.completed()
        }
      }

      val latch = new CountDownLatch(8)

      intercept[RuntimeException] {
        ds.to(new Sink {
          override def open(schema: StructType): SinkWriter = new SinkWriter {
            override def close(): Unit = latch.countDown()
            override def write(row: Row): Unit = sys.error("boom")
          }
        }, 8)
      }
    }
    "close all streams if multiple when an error in one" in {

      val _schema = StructType(Field("a"))
      val ds = new DataStream {
        override def schema: StructType = _schema
        override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
          Try {
            subscriber.subscribed(Subscription.empty)
            Iterator.continually(Row(schema, Seq("a"))).take(100000).grouped(100).foreach(subscriber.next)
            subscriber.completed()
          }
        }
      }

      val latch = new CountDownLatch(8)

      Try {
        ds.to(new Sink {
          override def open(schema: StructType): SinkWriter = new SinkWriter {
            override def close(): Unit = latch.countDown()
            override def write(row: Row): Unit = sys.error("boom")
          }
        }, 8)
      }

      latch.await(20, TimeUnit.SECONDS) shouldBe true
    }
  }
}

class ErrorSink extends Sink {
  override def open(schema: StructType): SinkWriter = new SinkWriter {
    override def close(): Unit = ()
    override def write(row: Row): Unit = sys.error("boom")
  }
}