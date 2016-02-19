package io.eels.component.json

import io.eels.Frame
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, WordSpec}

class JsonSinkTest extends WordSpec with Matchers {

  "JsonSink" should {
    "write multiple json docs to a file" in {

      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )

      val path = new Path("test")
      val fs = FileSystem.get(new Configuration)
      if (fs.exists(path))
        fs.delete(path, false)

      frame.to(JsonSink(path)).run
      IOUtils.toString(fs.open(path)) shouldBe
        """{"name":"sam","location":"aylesbury"}
          |{"name":"jam","location":"aylesbury"}
          |{"name":"ham","location":"buckingham"}
          |""".stripMargin
      fs.delete(path, false)
    }
  }
}

