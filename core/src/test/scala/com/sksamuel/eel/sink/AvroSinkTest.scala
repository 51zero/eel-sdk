package com.sksamuel.eel.sink

import java.io.ByteArrayOutputStream
import java.nio.file.{Paths, Files}

import com.sksamuel.avro4s.AvroInputStream
import com.sksamuel.eel.source.AvroFileWriter.Person
import com.sksamuel.eel.{Row, Sink, Frame}
import org.scalatest.{Matchers, WordSpec}

class AvroSinkTest extends WordSpec with Matchers {

  val frame = Frame(
    Row(Seq("name", "job", "location"), Seq("clint eastwood", "actor", "carmel")),
    Row(Seq("name", "job", "location"), Seq("elton john", "musician", "pinner")),
    Row(Seq("name", "job", "location"), Seq("issac newton", "scientist", "heaven"))
  )

  "AvroSink" should {
    "write to avro" in {
      val baos = new ByteArrayOutputStream()
      frame.to(AvroSink(baos))
      val in = AvroInputStream[Person](baos.toByteArray)
      in.iterator.toList shouldBe List(
        Person("clint eastwood", "actor", "carmel"),
        Person("elton john", "musician", "pinner"),
        Person("issac newton", "scientist", "heaven")
      )
    }
  }
}
