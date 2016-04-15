package io.eels.component.csv

import io.eels.Column
import io.eels.ColumnType
import io.eels.Schema
import java.nio.file.Paths

import io.kotlintest.specs.WordSpec

class CsvSourceTest : WordSpec() {

  //  init {
  //
  //    val file = javaClass.getResource("/csvtest.csv").toURI()
  //    val path = Paths.get(file)
  //
  //    "CsvSource" should {
  //      "read schema" with {
  //        CsvSource(path).schema() shouldBe Schema(listOf(
  //            Column("a", ColumnType.String, true),
  //            Column("b", ColumnType.String, true),
  //            Column("c", ColumnType.String, true)
  //        ))
  //      }
  //      "support reading empty values as null" with {
  //        val file = javaClass.getResource("/csvwithempty.csv").toURI()
  //        val path = Paths.get(file)
  //        CsvSource(path).withEmptyCellValue(null).toSet.map { it.values } shouldBe setOf(arrayOf("1", null, "3"))
  //      }
  //      "support reading empty values with replacement value" with {
  //        val file = javaClass.getResource("/csvwithempty.csv").toURI()
  //        val path = Paths.get(file)
  //        CsvSource(path).withEmptyCellValue("foo").toSet.map(_.values) shouldBe setOf(arrayOf("1", "foo", "3"))
  //      }
  //      "read from path" in {
  //        CsvSource(path).withHeader(Header.FirstRow).size shouldBe 3
  //        CsvSource(path).withHeader(Header.None).size shouldBe 4
  //      }
  //      "allow specifying manual schema" with {
  //        val schema = Schema(listOf(
  //            Column("test1", ColumnType.String, true),
  //            Column("test2", ColumnType.String, true),
  //            Column("test3", ColumnType.String, true))
  //        )
  //        CsvSource(path).withSchema(schema).drop(1).schema shouldBe schema
  //      }
  //      "support reading header" with {
  //        CsvSource(path).withHeader(Header.FirstRow).toSet.map(_.values) shouldBe
  //            setOf(listOf("e", "f", "g"), listOf("1", "2", "3"), listOf("4", "5", "6"))
  //      }
  //      "support skipping header" with {
  //        CsvSource(path).withHeader(Header.None).toSet.map(_.values) shouldBe
  //            setOf(listOf("a", "b", "c"), listOf("e", "f", "g"), listOf("1", "2", "3"), listOf("4", "5", "6"))
  //      }
  //      "support delimiters" with {
  //        val file = javaClass.getResource("/psv.psv").toURI()
  //        val path = Paths.get(file)
  //        CsvSource(path).withDelimiter('|').toSeq.map(_.values) shouldBe listOf(listOf("e", "f", "g"))
  //        CsvSource(path).withDelimiter('|').withHeader(Header.None).toSet.map(_.values) shouldBe setOf(listOf("a", "b", "c"), listOf("e", "f", "g"))
  //      }
  //      "support comments for headers" with {
  //        val file = javaClass.getResource("/comments.csv").toURI()
  //        val path = Paths.get(file)
  //        CsvSource(path).withHeader(Header.FirstComment).schema() shouldBe Schema(listOf(
  //            Column("a", ColumnType.String, true),
  //            Column("b", ColumnType.String, true),
  //            Column("c", ColumnType.String, true)
  //        ))
  //        CsvSource(path).withHeader(Header.FirstComment).toSet.map(_.values) shouldBe
  //            setOf(listOf("1", "2", "3"), listOf("e", "f", "g"), listOf("4", "5", "6"))
  //      }
  //      "terminate if asking for first comment but no comments" with {
  //        CsvSource(path).withHeader(Header.FirstComment).schema() shouldBe Schema(listOf(
  //            Column("", ColumnType.String, true)
  //        ))
  //      }
  //      "support verifying rows" with {
  //        val file = javaClass.getResource("/corrupt.csv").toURI()
  //        val path = Paths.get(file)
  //        CsvSource(path).withHeader(Header.FirstRow).toSeq.map { it.values } shouldBe listOf(listOf("1", "2", "3"))
  //      }
  //    }
  //  }
}