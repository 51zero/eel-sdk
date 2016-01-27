package com.fiftyonezero.eel.source

import java.nio.file.Paths

import com.fiftyonezero.eel.{FrameSchema, SchemaType, Column}
import org.scalatest.{Matchers, WordSpec}

class CsvSourceTest extends WordSpec with Matchers {

  val path = Paths.get(getClass.getResource("/csvtest.csv").getFile)

  "CsvSource" should {
    "read schema" in {
      CsvSource(path).schema shouldBe FrameSchema(List(Column("a"), Column("b"), Column("c")))
    }
    "read from path" in {
      CsvSource(path).size shouldBe 3
    }
    "allow specifying manual schema" in {
      val schema = FrameSchema(Seq(
        Column("test1", SchemaType.String, true),
        Column("test2", SchemaType.String, true),
        Column("test3", SchemaType.String, true))
      )
      CsvSource(path).withSchema(schema).drop(1).schema shouldBe schema
    }
  }
}
