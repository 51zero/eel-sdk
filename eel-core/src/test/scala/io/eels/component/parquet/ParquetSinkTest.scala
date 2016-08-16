package io.eels.component.parquet

import io.eels.Frame
import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class ParquetSinkTest extends WordSpec with Matchers {
  ParquetLogMute()

  val schema = Schema(
    Field("name", FieldType.String, nullable = false),
    Field("job", FieldType.String, nullable = false),
    Field("location", FieldType.String, nullable = false)
  )
  val frame = Frame(
    schema,
    Vector("clint eastwood", "actor", "carmel"),
    Vector("elton john", "musician", "pinner")
  )

  implicit val fs = FileSystem.get(new Configuration())
  val path = new Path("test.pq")

  "ParquetSink" should {
    "write schema" in {
      if (fs.exists(path))
        fs.delete(path, false)
      frame.to(ParquetSink(path))
      val people = ParquetSource(path)
      people.schema() shouldBe Schema(
        Field("name", FieldType.String, false),
        Field("job", FieldType.String, false),
        Field("location", FieldType.String, false)
      )
      fs.delete(path, false)
    }
    "write data" in {
      if (fs.exists(path))
        fs.delete(path, false)
      frame.to(ParquetSink(path))
      ParquetSource(path).toFrame(1).toSet().map(_.values) shouldBe
        Set(
          Vector("clint eastwood", "actor", "carmel"),
          Vector("elton john", "musician", "pinner")
        )
      fs.delete(path, false)
    }
  }
}