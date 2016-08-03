package io.eels.component.csv

import io.eels.Frame
import io.eels.Row
import io.eels.schema.Field
import io.eels.schema.Schema
import io.kotlintest.specs.WordSpec
import java.nio.charset.Charset
import java.nio.file.Files

class CsvSinkTest : WordSpec() {
  init {

    "CsvSink" should {
      "write csv data" {

        val schema = Schema(Field("name"), Field("job"), Field("location"))
        val frame = Frame(
            schema,
            Row(schema, listOf("clint eastwood", "actor", "carmel")),
            Row(schema, listOf("elton john", "musician", "pinner"))
        )

        val temp = Files.createTempFile("csvsink", ".test")
        frame.to(CsvSink(temp))
        val result = Files.readAllLines(temp, Charset.defaultCharset()).joinToString("\n")
        result shouldBe "clint eastwood,actor,carmel\nelton john,musician,pinner"
      }
      "support setting delimiter" {

        val schema = Schema(Field("name"), Field("job"), Field("location"))
        val frame = Frame(
            schema,
            Row(schema, listOf("clint eastwood", "actor", "carmel")),
            Row(schema, listOf("elton john", "musician", "pinner"))
        )

        val temp = Files.createTempFile("csvsink", ".test")
        frame.to(CsvSink(temp, format = CsvFormat().copy(delimiter = '>')))
        val result = Files.readAllLines(temp, Charset.defaultCharset()).joinToString("\n")
        result shouldBe "clint eastwood>actor>carmel\nelton john>musician>pinner"
      }
      "write headers when specified" {
        val schema = Schema(Field("name"), Field("job"), Field("location"))
        val frame = Frame(
            schema,
            Row(schema, listOf("clint eastwood", "actor", "carmel")),
            Row(schema, listOf("elton john", "musician", "pinner"))
        )
        val temp = Files.createTempFile("csvsink", ".test")
        frame.to(CsvSink(temp).withHeaders(Header.FirstRow))
        val result = Files.readAllLines(temp, Charset.defaultCharset()).joinToString("\n")
        result shouldBe "name,job,location\nclint eastwood,actor,carmel\nelton john,musician,pinner"
      }
      "write null values as empty strings" {
        val schema = Schema(Field("name"), Field("job"), Field("location"))
        val frame = Frame(
            schema,
            Row(schema, listOf("clint eastwood", null, "carmel")),
            Row(schema, listOf("elton john", null, "pinner"))
        )
        val temp = Files.createTempFile("csvsink", ".test")
        frame.to(CsvSink(temp))
        val result = Files.readAllLines(temp, Charset.defaultCharset()).joinToString("\n")
        result shouldBe "clint eastwood,,carmel\nelton john,,pinner"
      }
    }
  }
}
