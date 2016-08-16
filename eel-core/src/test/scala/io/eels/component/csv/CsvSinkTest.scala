package io.eels.component.csv

import java.nio.charset.Charset
import java.nio.file.Files

import io.eels.{Frame, Row}
import io.eels.schema.{Field, Schema}
import org.scalatest.{Matchers, WordSpec}
import scala.collection.JavaConverters._

class CsvSinkTest extends WordSpec with Matchers {

  "CsvSink" should {
    "write csv data" in {

      val schema = Schema(Field("name"), Field("job"), Field("location"))
      val frame = Frame(
        schema,
        Row(schema, Vector("clint eastwood", "actor", "carmel")),
        Row(schema, Vector("elton john", "musician", "pinner"))
      )

      val temp = Files.createTempFile("csvsink", ".test")
      frame.to(CsvSink(temp))
      val result = Files.readAllLines(temp, Charset.defaultCharset()).asScala.mkString("\n")
      result shouldBe "name,job,location\nclint eastwood,actor,carmel\nelton john,musician,pinner"
    }
    "support setting delimiter" in {

      val schema = Schema(Field("name"), Field("job"), Field("location"))
      val frame = Frame(
        schema,
        Row(schema, Vector("clint eastwood", "actor", "carmel")),
        Row(schema, Vector("elton john", "musician", "pinner"))
      )

      val temp = Files.createTempFile("csvsink", ".test")
      frame.to(CsvSink(temp, format = CsvFormat().copy(delimiter = '>')))
      val result = Files.readAllLines(temp, Charset.defaultCharset()).asScala.mkString("\n")
      result shouldBe "name>job>location\nclint eastwood>actor>carmel\nelton john>musician>pinner"
    }
    "write headers when specified" in {
      val schema = Schema(Field("name"), Field("job"), Field("location"))
      val frame = Frame(
        schema,
        Row(schema, Vector("clint eastwood", "actor", "carmel")),
        Row(schema, Vector("elton john", "musician", "pinner"))
      )
      val temp = Files.createTempFile("csvsink", ".test")
      frame.to(CsvSink(temp).withHeaders(Header.FirstRow))
      val result = Files.readAllLines(temp, Charset.defaultCharset()).asScala.mkString("\n")
      result shouldBe "name,job,location\nclint eastwood,actor,carmel\nelton john,musician,pinner"
    }
    "write null values as empty strings" in {
      val schema = Schema(Field("name"), Field("job"), Field("location"))
      val frame = Frame(
        schema,
        Row(schema, Vector("clint eastwood", null, "carmel")),
        Row(schema, Vector("elton john", null, "pinner"))
      )
      val temp = Files.createTempFile("csvsink", ".test")
      frame.to(CsvSink(temp))
      val result = Files.readAllLines(temp, Charset.defaultCharset()).asScala.mkString("\n")
      result shouldBe "name,job,location\nclint eastwood,,carmel\nelton john,,pinner"
    }
  }
}
