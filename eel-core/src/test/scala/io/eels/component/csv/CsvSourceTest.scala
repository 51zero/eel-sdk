package io.eels.component.csv

import java.nio.file.Paths

import io.eels.{Column, Schema, SchemaType}
import org.scalatest.{Matchers, WordSpec}

class CsvSourceTest extends WordSpec with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  val file = getClass.getResource("/csvtest.csv").toURI
  val path = Paths.get(file)

  "CsvSource" should {
    "read schema" in {
      CsvSource(path).schema shouldBe Schema(List(
        Column("a", SchemaType.String, true),
        Column("b", SchemaType.String, true),
        Column("c", SchemaType.String, true)
      ))
    }
    "read from path" in {
      CsvSource(path).size shouldBe 3
    }
    "allow specifying manual schema" in {
      val schema = Schema(List(
        Column("test1", SchemaType.String, true),
        Column("test2", SchemaType.String, true),
        Column("test3", SchemaType.String, true))
      )
      CsvSource(path).withSchema(schema).drop(1).schema shouldBe schema
    }
    "support reading header" in {
      CsvSource(path).withHeader(true).toSet.map(_.values) shouldBe
        Set(List("e", "f", "g"), List("1", "2", "3"), List("4", "5", "6"))
    }
    "support skipping header" in {
      CsvSource(path).withHeader(false).toSet.map(_.values) shouldBe
        Set(List("a", "b", "c"), List("e", "f", "g"), List("1", "2", "3"), List("4", "5", "6"))
    }
    "support schema inferrer" in {
      val inferrer = SchemaInferrer(SchemaType.String, SchemaRule("a", SchemaType.Int, false), SchemaRule("b", SchemaType.Boolean))
      CsvSource(path).withHeader(true).withSchemaInferrer(inferrer).schema shouldBe Schema(List(
        Column("a", SchemaType.Int, false),
        Column("b", SchemaType.Boolean, true),
        Column("c", SchemaType.String, true)
      ))
    }
  }
}
