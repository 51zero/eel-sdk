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

    val schema = Schema(Field("name"), Field("job"), Field("location"))

    val frame = Frame(
        schema,
        Row(schema, listOf("clint eastwood", "actor", "carmel")),
        Row(schema, listOf("elton john", "musician", "pinner")),
        Row(schema, listOf("david bowie", "musician", "surrey"))
    )

    "CsvSink" should {
      "write csv data" {
        val temp = Files.createTempFile("csvsink", ".test")
        frame.to(CsvSink(temp))
        val result = Files.readAllLines(temp, Charset.defaultCharset()).toSet()
        result shouldBe setOf("1,2,3,4", "5,6,7,8")
      }
      "support setting delimiter" {
        val temp = Files.createTempFile("csvsink", ".test")
        frame.to(CsvSink(temp, format = CsvFormat().copy(delimiter = '>')))
        val result = Files.readAllLines(temp, Charset.defaultCharset()).toSet()
        result shouldBe setOf("1>2>3>4", "5>6>7>8")
      }
      "write null values as empty strings" {
        val schema = Schema(Field("name"), Field("job"), Field("location"))
        val frame = Frame(
            schema,
            Row(schema, listOf("clint eastwood", null, "carmel")),
            Row(schema, listOf("elton john", null, "pinner")),
            Row(schema, listOf("david bowie", null, "surrey"))
        )
        val temp = Files.createTempFile("csvsink", ".test")
        frame.to(CsvSink(temp, format = CsvFormat().copy(delimiter = '>')))
        val result = Files.readAllLines(temp, Charset.defaultCharset()).toSet()
        result shouldBe setOf("1>2>3>4", "5>6>7>8")
      }
    }
  }
}
