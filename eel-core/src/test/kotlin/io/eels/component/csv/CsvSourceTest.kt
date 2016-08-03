@file:Suppress("NAME_SHADOWING")

package io.eels.component.csv

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Schema
import java.nio.file.Paths

import io.kotlintest.specs.WordSpec

class CsvSourceTest : WordSpec() {
  init {


    "CsvSource" should {
      "read schema"  {
        val file = javaClass.getResource("/csvtest.csv").toURI()
        val path = Paths.get(file)
        CsvSource(path).schema() shouldBe Schema(
            Field("a", FieldType.String, true),
            Field("b", FieldType.String, true),
            Field("c", FieldType.String, true)
        )
      }
      "support null cell value option as null"  {
        val file = javaClass.getResource("/csvwithempty.csv").toURI()
        val path = Paths.get(file)
        CsvSource(path).withNullValue(null).toFrame(1).toSet().map { it.values }.toSet() shouldBe
            setOf(listOf("1", null, "3"))
      }
      "support null cell value replacement value"  {
        val file = javaClass.getResource("/csvwithempty.csv").toURI()
        val path = Paths.get(file)
        CsvSource(path).withNullValue("foo").toFrame(1).toSet().map { it.values }.toSet() shouldBe
            setOf(listOf("1", "foo", "3"))
      }
      "read from path"  {
        val file = javaClass.getResource("/csvtest.csv").toURI()
        val path = Paths.get(file)
        CsvSource(path).withHeader(Header.FirstRow).toFrame(1).size() shouldBe 3
        CsvSource(path).withHeader(Header.None).toFrame(1).size() shouldBe 4
      }
      "allow specifying manual schema"  {
        val file = javaClass.getResource("/csvtest.csv").toURI()
        val path = Paths.get(file)
        val schema = Schema(
            Field("test1", FieldType.String, true),
            Field("test2", FieldType.String, true),
            Field("test3", FieldType.String, true)
        )
        CsvSource(path).withSchema(schema).toFrame(1).schema() shouldBe schema
      }
      "support reading header"  {
        val file = javaClass.getResource("/csvtest.csv").toURI()
        val path = Paths.get(file)
        CsvSource(path).withHeader(Header.FirstRow).toFrame(1).toList().map { it.values }.toSet() shouldBe
            setOf(listOf("e", "f", "g"), listOf("1", "2", "3"), listOf("4", "5", "6"))
      }
      "support skipping header"  {
        val file = javaClass.getResource("/csvtest.csv").toURI()
        val path = Paths.get(file)
        CsvSource(path).withHeader(Header.None).toFrame(1).toSet().map { it.values }.toSet() shouldBe
            setOf(listOf("a", "b", "c"), listOf("e", "f", "g"), listOf("1", "2", "3"), listOf("4", "5", "6"))
      }
      "support delimiters"  {
        val file = javaClass.getResource("/psv.psv").toURI()
        val path = Paths.get(file)
        CsvSource(path).withDelimiter('|').toFrame(1).toList().map { it.values }.toSet() shouldBe
            setOf(listOf("e", "f", "g"))
        CsvSource(path).withDelimiter('|').withHeader(Header.None).toFrame(1).toSet().map { it.values }.toSet() shouldBe
            setOf(listOf("a", "b", "c"), listOf("e", "f", "g"))
      }
      "support comments for headers"  {
        val file = javaClass.getResource("/comments.csv").toURI()
        val path = Paths.get(file)
        CsvSource(path).withHeader(Header.FirstComment).schema() shouldBe Schema(
            Field("a", FieldType.String, true),
            Field("b", FieldType.String, true),
            Field("c", FieldType.String, true)
        )
        CsvSource(path).withHeader(Header.FirstComment).toFrame(1).toSet().map { it.values }.toSet() shouldBe
            setOf(listOf("1", "2", "3"), listOf("e", "f", "g"), listOf("4", "5", "6"))
      }
      "terminate if asking for first comment but no comments"  {
        val file = javaClass.getResource("/csvtest.csv").toURI()
        val path = Paths.get(file)
        CsvSource(path).withHeader(Header.FirstComment).schema() shouldBe Schema(
            Field("", FieldType.String, true)
        )
      }
      "support verifying rows"  {
        val file = javaClass.getResource("/corrupt.csv").toURI()
        val path = Paths.get(file)
        CsvSource(path).withHeader(Header.FirstRow).toFrame(1).toList().map { it.values } shouldBe
            listOf(listOf("1", "2", "3"))
      }
    }
  }
}