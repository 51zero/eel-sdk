package io.eels.component.avro

import java.io.ByteArrayOutputStream

import io.eels.Frame
import io.eels.schema.Schema
import org.scalatest.{Matchers, WordSpec}

class AvroSinkTest extends WordSpec with Matchers {

  val frame = Frame.fromValues(
    Schema("name", "job", "location"),
    List("clint eastwood", "actor", "carmel"),
    List("elton john", "musician", "pinner"),
    List("issac newton", "scientist", "heaven")
  )

  "AvroSink" should {
    "write to avro" in {
      val baos = new ByteArrayOutputStream()
      frame.to(AvroSink(baos))
    }
  }
}
