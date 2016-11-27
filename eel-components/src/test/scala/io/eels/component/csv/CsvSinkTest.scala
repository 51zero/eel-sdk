package io.eels.component.csv

import io.eels.schema.{Field, StructType}
import io.eels.{Frame, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, WordSpec}

import scala.io.Source

class CsvSinkTest extends WordSpec with Matchers {

  implicit val fs = FileSystem.getLocal(new Configuration())

  "CsvSink" should {
    "write csv data" in {

      val schema = StructType(Field("name"), Field("job"), Field("location"))
      val frame = Frame(
        schema,
        Row(schema, Vector("clint eastwood", "actor", "carmel")),
        Row(schema, Vector("elton john", "musician", "pinner"))
      )

      val temp = new Path("temp")
      frame.to(CsvSink(temp))
      val result = Source.fromInputStream(fs.open(temp)).mkString
      result shouldBe "name,job,location\nclint eastwood,actor,carmel\nelton john,musician,pinner\n"
    }
    "support setting delimiter" in {

      val schema = StructType(Field("name"), Field("job"), Field("location"))
      val frame = Frame(
        schema,
        Row(schema, Vector("clint eastwood", "actor", "carmel")),
        Row(schema, Vector("elton john", "musician", "pinner"))
      )

      val temp = new Path("temp")
      frame.to(CsvSink(temp, format = CsvFormat().copy(delimiter = '>')))
      val result = Source.fromInputStream(fs.open(temp)).mkString
      result shouldBe "name>job>location\nclint eastwood>actor>carmel\nelton john>musician>pinner\n"
    }
    "write headers when specified" in {
      val schema = StructType(Field("name"), Field("job"), Field("location"))
      val frame = Frame(
        schema,
        Row(schema, Vector("clint eastwood", "actor", "carmel")),
        Row(schema, Vector("elton john", "musician", "pinner"))
      )
      val temp = new Path("temp")
      frame.to(CsvSink(temp).withHeaders(Header.FirstRow))
      val result = Source.fromInputStream(fs.open(temp)).mkString
      result shouldBe "name,job,location\nclint eastwood,actor,carmel\nelton john,musician,pinner\n"
    }
    "write null values as empty strings" in {
      val schema = StructType(Field("name"), Field("job"), Field("location"))
      val frame = Frame(
        schema,
        Row(schema, Vector("clint eastwood", null, "carmel")),
        Row(schema, Vector("elton john", null, "pinner"))
      )
      val temp = new Path("temp")
      frame.to(CsvSink(temp))
      val result = Source.fromInputStream(fs.open(temp)).mkString
      result shouldBe "name,job,location\nclint eastwood,,carmel\nelton john,,pinner\n"
    }
  }
}
