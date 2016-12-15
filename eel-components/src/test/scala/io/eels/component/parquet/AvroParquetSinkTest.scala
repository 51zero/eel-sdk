package io.eels.component.parquet

import io.eels.Frame
import io.eels.component.parquet.avro.AvroParquetSink
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
      ParquetSource(path).toFrame().toSet().map(_.values) shouldBe
        Set(
          Vector("clint eastwood", "actor", "carmel"),
          Vector("elton john", "musician", "pinner")
        )
      fs.delete(path, false)
    }
  }
}