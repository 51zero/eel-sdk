package io.eels.component.parquet

import io.eels.{Column, Frame, FrameSchema}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

class ParquetSinkTest extends WordSpec with Matchers {

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
      people.schema shouldBe FrameSchema(List(Column("name"), Column("job"), Column("location")))
      fs.delete(path, false)
    }
    "write data" in {
      if (fs.exists(path))
        fs.delete(path, false)
      frame.to(ParquetSink(path))
      val people = ParquetSource(path)
      people.toSet.map(_.map(_.toString)) shouldBe
        Set(
          List("clint eastwood", "actor", "carmel"),
          List("elton john", "musician", "pinner")
        )
      fs.delete(path, false)
    }
  }
}

