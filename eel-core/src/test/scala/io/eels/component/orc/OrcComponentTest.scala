package io.eels.component.orc

import io.eels.Frame
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.scalatest.{Matchers, WordSpec}

class OrcComponentTest extends WordSpec with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  "OrcComponent" should {
    "read and write orc files" in {

      implicit val fs = FileSystem.get(new Configuration)

      val frame = Frame(
        List("name", "job", "location"),
        List("clint eastwood", "actor", "carmel"),
        List("elton john", "musician", "pinner"),
        List("david bowie", "musician", "surrey")
      )

      val path = new Path("test.orc")
      frame.to(OrcSink(path))

      val rows = OrcSource(path).toSet
      fs.delete(path, false)

      rows shouldBe Set(
        List("clint eastwood", "actor", "carmel"),
        List("elton john", "musician", "pinner"),
        List("david bowie", "musician", "surrey")
      )

    }
  }
}
