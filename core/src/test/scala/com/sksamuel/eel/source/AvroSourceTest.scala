package com.sksamuel.eel.source

import java.io.File
import java.nio.file.Paths

import com.sksamuel.eel.sink.{Column, Row}
import org.scalatest.{Matchers, WordSpec}

class AvroSourceTest extends WordSpec with Matchers {

  "AvroSource" should {
    "read avro files" in {
      val people = AvroSource(Paths.get(new File(getClass.getResource("/test.avro").getFile).getAbsolutePath)).toList
      people shouldBe List(
        Row(Seq("name", "job", "location"), Seq("clint eastwood", "actor", "carmel")),
        Row(Seq("name", "job", "location"), Seq("elton john", "musician", "pinner")),
        Row(Seq("name", "job", "location"), Seq("issac newton", "scientist", "heaven"))
      )
    }
  }
}

