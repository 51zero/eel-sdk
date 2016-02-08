package io.eels.component.avro

import java.io.File
import java.nio.file.Paths

import io.eels.{Column, FrameSchema, Row}
import org.scalatest.{Matchers, WordSpec}

class AvroSourceTest extends WordSpec with Matchers {

  "AvroSource" should {
    "read schema" in {
      val people = AvroSource(Paths.get(new File(getClass.getResource("/test.avro").getFile).getAbsolutePath))
      people.schema shouldBe FrameSchema(List(Column("name"), Column("job"), Column("location")))
    }
    "read avro files" in {
      val people = AvroSource(Paths.get(new File(getClass.getResource("/test.avro").getFile).getAbsolutePath)).toList
        .runConcurrent(2)
      people shouldBe List(
        Row(List("name", "job", "location"), List("clint eastwood", "actor", "carmel")),
        Row(List("name", "job", "location"), List("elton john", "musician", "pinner")),
        Row(List("name", "job", "location"), List("issac newton", "scientist", "heaven"))
      )
    }
  }
}

