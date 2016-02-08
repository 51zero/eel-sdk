package io.eels.component.hive

import io.eels.{Frame, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, WordSpec}

class OrcComponentTest extends WordSpec with Matchers {

  "OrcComponent" should {
    "read and write orc files" in {

      implicit val fs = FileSystem.get(new Configuration)

      val frame = Frame(
        Row(List("name", "job", "location"), List("clint eastwood", "actor", "carmel")),
        Row(List("name", "job", "location"), List("elton john", "musician", "pinner")),
        Row(List("name", "job", "location"), List("david bowie", "musician", "surrey"))
      )

      val path = new Path("test.orc")
      frame.to(OrcSink(path))

      val rows = OrcSource(path).toList.run
      fs.delete(path, false)

      rows.map(_.fields.map(_.value)) shouldBe Seq(
        List("clint eastwood", "actor", "carmel"),
        List("elton john", "musician", "pinner"),
        List("david bowie", "musician", "surrey")
      )

    }
  }
}
