package com.sksamuel.eel.component.avro

import com.sksamuel.eel.{Frame, Row}
import org.scalatest.{WordSpec, Matchers}

class AvroSinkTest extends WordSpec with Matchers {

  val frame = Frame(
    Row(Seq("name", "job", "location"), Seq("clint eastwood", "actor", "carmel")),
    Row(Seq("name", "job", "location"), Seq("elton john", "musician", "pinner")),
    Row(Seq("name", "job", "location"), Seq("issac newton", "scientist", "heaven"))
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
