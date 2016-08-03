package io.eels.component.json

import io.eels.Row
import io.eels.schema.Field
import io.eels.schema.Schema
import io.kotlintest.specs.WordSpec
import org.apache.hadoop.fs.Path

class JsonSourceTest : WordSpec() {
  init {
    "JsonSource" should {
      "read multiple json docs from a file" {
        val schema = Schema(Field("name"), Field("location"))
        JsonSource(Path(javaClass.getResource("/test.json").file)).toFrame(1).toSet() shouldBe
            setOf(
                Row(schema, "sammy", "aylesbury"),
                Row(schema, "ant", "greece")
            )
      }
    }
  }
}