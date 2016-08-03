package io.eels.component.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class ParquetSinkTest extends WordSpec with Matchers {
  ParquetLogMute()

  val frame = Frame(
    List("name", "job", "location"),
    List("clint eastwood", "actor", "carmel"),
    List("elton john", "musician", "pinner")
  )

  implicit val fs = FileSystem.get(new Configuration)
  val path = new Path("test.pq")

  "ParquetSink" should {
    "write schema" in {
      if (fs.exists(path))
        fs.delete(path, false)
      frame.to(ParquetSink(path))
      val people = ParquetSource(path)
      people.schema shouldBe {
        Schema(List(
          Column("name", SchemaType.String, false),
          Column("job", SchemaType.String, false),
          Column("location", SchemaType.String, false)))
      }
      fs.delete(path, false)
    }
    "write data" in {
      if (fs.exists(path))
        fs.delete(path, false)
      frame.to(ParquetSink(path))
      val people = ParquetSource(path)
      people.toSet.map(_.values.map(_.toString)) shouldBe
        Set(
          List("clint eastwood", "actor", "carmel"),
          List("elton john", "musician", "pinner")
        )
      fs.delete(path, false)
    }
  }
}

