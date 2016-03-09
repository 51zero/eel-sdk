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
    "support reading empty values as null" in {
      val file = getClass.getResource("/csvwithempty.csv").toURI
      val path = Paths.get(file)
      CsvSource(path).withEmptyCellValue(null).toSet.map(_.values) shouldBe Set(Seq("1", null, "3"))
    }
    "support reading empty values with replacement value" in {
      val file = getClass.getResource("/csvwithempty.csv").toURI
      val path = Paths.get(file)
      CsvSource(path).withEmptyCellValue("foo").toSet.map(_.values) shouldBe Set(Seq("1", "foo", "3"))
    }
    "read from path" in {
      CsvSource(path).withHeader(Header.FirstRow).size shouldBe 3
      CsvSource(path).withHeader(Header.None).size shouldBe 4
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
      CsvSource(path).withHeader(Header.FirstRow).toSet.map(_.values) shouldBe
        Set(List("e", "f", "g"), List("1", "2", "3"), List("4", "5", "6"))
    }
    "support skipping header" in {
      CsvSource(path).withHeader(Header.None).toSet.map(_.values) shouldBe
        Set(List("a", "b", "c"), List("e", "f", "g"), List("1", "2", "3"), List("4", "5", "6"))
    }
    "support schema inferrer" in {
      val inferrer = SchemaInferrer(SchemaType.String, SchemaRule("a", SchemaType.Int, false), SchemaRule("b", SchemaType.Boolean))
      CsvSource(path).withHeader(Header.FirstRow).withSchemaInferrer(inferrer).schema shouldBe Schema(List(
        Column("a", SchemaType.Int, false),
        Column("b", SchemaType.Boolean, true),
        Column("c", SchemaType.String, true)
      ))
    }
    "support delimiters" in {
      val file = getClass.getResource("/psv.psv").toURI
      val path = Paths.get(file)
      CsvSource(path).withDelimiter('|').toSeq.map(_.values) shouldBe Seq(Seq("e", "f", "g"))
      CsvSource(path).withDelimiter('|').withHeader(Header.None).toSet.map(_.values) shouldBe Set(Seq("a", "b", "c"), Seq("e", "f", "g"))
    }
    "support comments for headers" in {
      val file = getClass.getResource("/comments.csv").toURI
      val path = Paths.get(file)
      CsvSource(path).withHeader(Header.FirstComment).schema shouldBe Schema(List(
        Column("a", SchemaType.String, true),
        Column("b", SchemaType.String, true),
        Column("c", SchemaType.String, true)
      ))
      CsvSource(path).withHeader(Header.FirstComment).toSet.map(_.values) shouldBe
        Set(Seq("1", "2", "3"), Seq("e", "f", "g"), Seq("4", "5", "6"))
    }
    "terminate if asking for first comment but no comments" in {
      CsvSource(path).withHeader(Header.FirstComment).schema shouldBe Schema(List(
        Column("", SchemaType.String, true)
      ))
    }
    "support verifying rows" in {
      val file = getClass.getResource("/corrupt.csv").toURI
      val path = Paths.get(file)
      CsvSource(path).withHeader(Header.FirstRow).toSeq.map(_.values) shouldBe Vector(Seq("1", "2", "3"))
    }
  }
}
