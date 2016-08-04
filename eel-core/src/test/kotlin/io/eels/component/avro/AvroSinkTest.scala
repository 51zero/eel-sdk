package io.eels.component.avro

import io.kotlintest.matchers.Matchers
import io.kotlintest.specs.WordSpec

class AvroSinkTest extends WordSpec with Matchers {

  val frame = Frame(
    List("name", "job", "location"),
    List("clint eastwood", "actor", "carmel"),
    List("elton john", "musician", "pinner"),
    List("issac newton", "scientist", "heaven")
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
