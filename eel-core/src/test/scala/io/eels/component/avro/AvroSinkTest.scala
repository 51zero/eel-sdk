package io.eels.component.avro

import io.eels.{Frame, Row}
import org.scalatest.{Matchers, WordSpec}

class AvroSinkTest extends WordSpec with Matchers {

  val frame = Frame(
    Row(List("name", "job", "location"), List("clint eastwood", "actor", "carmel")),
    Row(List("name", "job", "location"), List("elton john", "musician", "pinner")),
    Row(List("name", "job", "location"), List("issac newton", "scientist", "heaven"))
  )

//  "AvroSink" should {
//    "write to avro" in {
//      val baos = new ByteArrayOutputStream()
//      frame.to(AvroSink(baos))
//      val in = AvroInputStream[Person](baos.toByteArray)
//      in.iterator.toList shouldBe List(
//        Person("clint eastwood", "actor", "carmel"),
//        Person("elton john", "musician", "pinner"),
//        Person("issac newton", "scientist", "heaven")
//      )
//    }
//  }
}
