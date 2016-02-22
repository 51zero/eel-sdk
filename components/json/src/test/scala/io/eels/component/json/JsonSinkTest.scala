package io.eels.component.json

import io.eels.Frame
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, WordSpec}

class JsonSinkTest extends WordSpec with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

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

      frame.to(JsonSink(path))
      val input = IOUtils.toString(fs.open(path))
      input should include ("""{"name":"sam","location":"aylesbury"}""")
      input should include ("""{"name":"jam","location":"aylesbury"}""")
      input should include ("""{"name":"ham","location":"buckingham"}""")
      fs.delete(path, false)
    }
  }
}

