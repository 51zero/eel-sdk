package io.eels.component.orc

import io.eels.{Frame, Schema, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, WordSpec}

class OrcComponentTest extends WordSpec with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  "OrcComponent" should {
    "read and write orc files" in {

      implicit val fs = FileSystem.get(new Configuration)

      val frame = Frame(
        Schema("name", "job", "location"),
        Vector("clint eastwood", "actor", "carmel"),
        Vector("elton john", "musician", "pinner"),
        Vector("david bowie", "musician", "surrey")
      )

      val path = new Path("test.orc")
      frame.to(OrcSink(path))

      val rows = OrcSource(path).toSet.toSet
      fs.delete(path, false)

      rows shouldBe Set(
        Row(frame.schema, "clint eastwood", "actor", "carmel"),
        Row(frame.schema, "elton john", "musician", "pinner"),
        Row(frame.schema, "david bowie", "musician", "surrey")
      )

    }
  }
}