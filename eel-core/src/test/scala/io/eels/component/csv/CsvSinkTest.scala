package io.eels.component.csv

import java.util.UUID

import io.eels.Row
import io.eels.component.parquet.ParquetSink
import io.eels.datastream.DataStream
import io.eels.schema.{Field, StringType, StructType}
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
      val frame = DataStream.fromRows(
        schema,
        Row(schema, Vector("clint eastwood", "actor", "carmel")),
        Row(schema, Vector("elton john", "musician", "pinner"))
      )

      frame.to(CsvSink(temp))
      val result = Source.fromInputStream(fs.open(temp)).mkString.replace("\r", "")
      result shouldBe "name,job,location\nclint eastwood,actor,carmel\nelton john,musician,pinner\n"
    }
    "support setting delimiter" in {

      val schema = StructType(Field("name"), Field("job"), Field("location"))
      val frame = DataStream.fromRows(
        schema,
        Row(schema, Vector("clint eastwood", "actor", "carmel")),
        Row(schema, Vector("elton john", "musician", "pinner"))
      )

      frame.to(CsvSink(temp, format = CsvFormat().copy(delimiter = '>')))
      val result = Source.fromInputStream(fs.open(temp)).mkString.replace("\r", "")
      result shouldBe "name>job>location\nclint eastwood>actor>carmel\nelton john>musician>pinner\n"
    }
    "write headers when specified" in {
      val schema = StructType(Field("name"), Field("job"), Field("location"))
      val frame = DataStream.fromRows(
        schema,
        Row(schema, Vector("clint eastwood", "actor", "carmel")),
        Row(schema, Vector("elton john", "musician", "pinner"))
      )
      frame.to(CsvSink(temp).withHeaders(Header.FirstRow))
      val result = Source.fromInputStream(fs.open(temp)).mkString.replace("\r", "")
      result shouldBe "name,job,location\nclint eastwood,actor,carmel\nelton john,musician,pinner\n"
    }
    "write null values as empty strings" in {
      val schema = StructType(Field("name"), Field("job"), Field("location"))
      val frame = DataStream.fromRows(
        schema,
        Row(schema, Vector("clint eastwood", null, "carmel")),
        Row(schema, Vector("elton john", null, "pinner"))
      )
      frame.to(CsvSink(temp))
      val result = Source.fromInputStream(fs.open(temp)).mkString.replace("\r", "")
      result shouldBe "name,job,location\nclint eastwood,,carmel\nelton john,,pinner\n"
    }
    "support overwrite" in {
      val path = new Path(s"target/${UUID.randomUUID().toString}", s"${UUID.randomUUID().toString}.csv")
      val schema = StructType(Field("a", StringType))
      val ds = DataStream.fromRows(
        schema,
        Seq(
          Row(schema, Vector("x")),
          Row(schema, Vector("y"))
        )
      )

      // Write twice to test overwrite
      ds.to(CsvSink(path))
      ds.to(CsvSink(path).withOverwrite(true))

      var parentStatus = fs.listStatus(path.getParent)
      println("CSV Overwrite:")
      parentStatus.foreach(p => println(p.getPath))
      parentStatus.length shouldBe 1
      parentStatus.head.getPath.getName shouldBe path.getName

      // Write again without overwrite
      val appendPath = new Path(path.getParent, s"${UUID.randomUUID().toString}.csv")
      ds.to(CsvSink(appendPath).withOverwrite(false))
      parentStatus = fs.listStatus(path.getParent)
      println("CSV Append:")
      parentStatus.foreach(p => println(p.getPath))
      parentStatus.length shouldBe 2

      // Write overwrite the same file again - it should still be 2
      ds.to(CsvSink(path).withOverwrite(true))
      parentStatus = fs.listStatus(path.getParent)
      println("CSV Again Overwrite:")
      parentStatus.foreach(p => println(p.getPath))
      parentStatus.length shouldBe 2
      parentStatus.head.getPath.getName shouldBe path.getName

    }
  }
}
