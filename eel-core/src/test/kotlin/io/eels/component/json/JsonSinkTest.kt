package io.eels.component.json

import io.eels.Frame
import io.eels.schema.Field
import io.eels.schema.Schema
import io.kotlintest.matchers.have
import io.kotlintest.specs.WordSpec
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

class JsonSinkTest : WordSpec() {
  init {
    "JsonSink" should {
      "write multiple json docs to a file" {

        val schema = Schema(Field("name"), Field("location"))
        val frame = Frame(
            schema,
            listOf("sam", "aylesbury"),
            listOf("jam", "aylesbury"),
            listOf("ham", "buckingham")
        )

        val path = Path("test")
        val fs = FileSystem.get(Configuration())
        if (fs.exists(path))
          fs.delete(path, false)

        frame.to(JsonSink(path))
        val input = IOUtils.toString(fs.open(path))
        input should have substring ("""{"name":"sam","location":"aylesbury"}""")
        input should have substring ("""{"name":"jam","location":"aylesbury"}""")
        input should have substring ("""{"name":"ham","location":"buckingham"}""")
        fs.delete(path, false)
      }
    }
  }
}