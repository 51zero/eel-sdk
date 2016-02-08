package io.eels.component.json

import io.eels.{Frame, Row}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, WordSpec}

class JsonSinkTest extends WordSpec with Matchers {

  "JsonSink" should {
    "write multiple json docs to a file" in {
      val row1 = Row(Map("a" -> "sam", "b" -> "ham"))
      val row2 = Row(Map("a" -> "tim", "b" -> "lim"))

      val path = new Path("test")
      val fs = FileSystem.get(new Configuration)
      if (fs.exists(path))
        fs.delete(path, false)
      Frame(row1, row2).to(JsonSink(path)).run
      IOUtils.toString(fs.open(path)) shouldBe """{"a":"sam","b":"ham"}""" + "\n" +"""{"a":"tim","b":"lim"}""" + "\n"
      fs.delete(path, false)
    }
  }
}

