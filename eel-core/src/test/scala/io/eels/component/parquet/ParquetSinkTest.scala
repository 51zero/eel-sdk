package io.eels.component.parquet

import io.eels.{Column, Frame, Schema, SchemaType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, WordSpec}

class ParquetSinkTest extends WordSpec with Matchers {
  ParquetLogMute()

  import scala.concurrent.ExecutionContext.Implicits.global

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
          Column("name", SchemaType.String, true, 0, 0, true, None),
          Column("job", SchemaType.String, true, 0, 0, true, None),
          Column("location", SchemaType.String, true, 0, 0, true, None)))
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

