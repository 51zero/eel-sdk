package io.eels.datastream

import io.eels.{Row, SinkWriter, Sink}
import io.eels.schema.{Field, StructType}
import org.scalatest.{Matchers, WordSpec}

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
    "handle errors when writing" in {

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

        closed shouldBe true
      }
    }
  }
}
