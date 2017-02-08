package io.eels.component.json

import io.eels.Frame
import io.eels.schema.{Field, StructType}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, WordSpec}

class JsonSinkTest extends WordSpec with Matchers {

  val path = new Path("test.json")
  implicit val fs: FileSystem = FileSystem.get(new Configuration())

  "JsonSink" should {
    "write multiple json docs to a file" in {
      if (fs.exists(path))
        fs.delete(path, false)

      val schema = StructType(Field("name"), Field("location"))
      val frame = Frame.fromValues(
        schema,
        Vector("sam", "aylesbury"),
        Vector("jam", "aylesbury"),
        Vector("ham", "buckingham")
      )

      frame.to(JsonSink(path))
      val input = IOUtils.toString(fs.open(path))
      input should include("""{"name":"sam","location":"aylesbury"}""")
      input should include("""{"name":"jam","location":"aylesbury"}""")
      input should include("""{"name":"ham","location":"buckingham"}""")

      fs.delete(path, false)
    }
    "support arrays" in {
      if (fs.exists(path))
        fs.delete(path, false)

      val schema = StructType(Field("name"), Field("skills"))
      val frame = Frame.fromValues(
        schema,
        Vector("sam", Array("karate", "kung fu"))
      )

      frame.to(JsonSink(path))
      val input = IOUtils.toString(fs.open(path))
      input.trim shouldBe """{"name":"sam","skills":["karate","kung fu"]}"""

      fs.delete(path, false)
    }
    "support maps" in {
      if (fs.exists(path))
        fs.delete(path, false)

      val schema = StructType(Field("name"), Field("locations"))
      val frame = Frame.fromValues(
        schema,
        Vector("sam", Map("home" -> "boro", "work" -> "london"))
      )

      frame.to(JsonSink(path))
      val input = IOUtils.toString(fs.open(path))
      input.trim shouldBe """{"name":"sam","locations":{"home":"boro","work":"london"}}"""

      fs.delete(path, false)
    }
    "support structs" in {

      case class Foo(home: String, work: String)

      if (fs.exists(path))
        fs.delete(path, false)

      val schema = StructType(Field("name"), Field("locations"))
      val frame = Frame.fromValues(
        schema,
        Vector("sam", Foo("boro", "london"))
      )

      frame.to(JsonSink(path))
      val input = IOUtils.toString(fs.open(path))
      input.trim shouldBe """{"name":"sam","locations":{"home":"boro","work":"london"}}"""

      fs.delete(path, false)
    }
  }
}