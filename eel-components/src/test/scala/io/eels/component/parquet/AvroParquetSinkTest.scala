package io.eels.component.parquet

import io.eels.component.csv.CsvSink
import io.eels.{Frame, Row}
import io.eels.component.parquet.avro.{AvroParquetSink, AvroParquetSource}
import io.eels.component.parquet.util.ParquetLogMute
import io.eels.schema.{Field, StringType, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, WordSpec}

class AvroParquetSinkTest extends WordSpec with Matchers {
  ParquetLogMute()

  val schema = StructType(
    Field("name", StringType, nullable = false),
    Field("job", StringType, nullable = false),
    Field("location", StringType, nullable = false)
  )
  val frame = Frame.fromValues(
    schema,
    Vector("clint eastwood", "actor", "carmel"),
    Vector("elton john", "musician", "pinner")
  )

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.get(new Configuration())
  val path = new Path("test.pq")

  "ParquetSink" should {
    "write schema" in {
      if (fs.exists(path))
        fs.delete(path, false)
      frame.to(AvroParquetSink(path))
      val people = ParquetSource(path)
      people.schema shouldBe StructType(
        Field("name", StringType, false),
        Field("job", StringType, false),
        Field("location", StringType, false)
      )
      fs.delete(path, false)
    }
    "write data" in {
      if (fs.exists(path))
        fs.delete(path, false)
      frame.to(AvroParquetSink(path))
      AvroParquetSource(path).toFrame().toSet().map(_.values) shouldBe
        Set(
          Vector("clint eastwood", "actor", "carmel"),
          Vector("elton john", "musician", "pinner")
        )
      fs.delete(path, false)
    }
    "support overwrite" in {

      val path = new Path("overwrite_test.pq")
      fs.delete(path, false)

      val schema = StructType(Field("a", StringType))
      val frame = Frame(schema,
        Row(schema, Vector("x")),
        Row(schema, Vector("y"))
      )

      frame.to(AvroParquetSink(path))
      frame.to(AvroParquetSink(path).withOverwrite(true))
      fs.delete(path, false)
    }
  }
}