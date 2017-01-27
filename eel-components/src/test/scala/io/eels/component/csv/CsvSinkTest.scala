package io.eels.component.csv

import io.eels.schema.{Field, StringType, StructType}
import io.eels.{Frame, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.io.Source

class CsvSinkTest extends WordSpec with Matchers with BeforeAndAfter {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(conf)

  val temp = new Path("temp.csv")

  before {
    if (fs.exists(temp)) fs.delete(temp, false)
  }

  after {
    if (fs.exists(temp)) fs.delete(temp, false)
  }

  "CsvSink" should {
    "write csv data" in {

      val schema = StructType(Field("name"), Field("job"), Field("location"))
      val frame = Frame(
        schema,
        Row(schema, Vector("clint eastwood", "actor", "carmel")),
        Row(schema, Vector("elton john", "musician", "pinner"))
      )

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
      frame.to(CsvSink(temp))
      val result = Source.fromInputStream(fs.open(temp)).mkString
      result shouldBe "name,job,location\nclint eastwood,,carmel\nelton john,,pinner\n"
    }
    "support overwrite" in {

      val path = new Path("overwrite_test.csv")
      fs.delete(path, false)

      val schema = StructType(Field("a", StringType))
      val frame = Frame(schema,
        Row(schema, Vector("x")),
        Row(schema, Vector("y"))
      )

      frame.to(CsvSink(path))
      frame.to(CsvSink(path).withOverwrite(true))
      fs.delete(path, false)
    }
  }
}
