package io.eels.component.csv

import java.nio.file.Paths

import io.eels.{Column, FrameSchema, SchemaType}
import org.scalatest.{Matchers, WordSpec}

class CsvSourceTest extends WordSpec with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  val file = getClass.getResource("/csvtest.csv").toURI
  val path = Paths.get(file)

  "CsvSource" should {
    "read schema" in {
      CsvSource(path).schema shouldBe FrameSchema(List(Column("a"), Column("b"), Column("c")))
    }
    "read from path" in {
      CsvSource(path).size shouldBe 3
    }
    "allow specifying manual schema" in {
      val schema = FrameSchema(List(
        Column("test1", SchemaType.String, true),
        Column("test2", SchemaType.String, true),
        Column("test3", SchemaType.String, true))
      )
      CsvSource(path).withSchema(schema).drop(1).schema shouldBe schema
    }
  }
}
