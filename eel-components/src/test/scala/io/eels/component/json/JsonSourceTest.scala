package io.eels.component.json

import io.eels.Row
import io.eels.schema.Field
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, WordSpec}

class JsonSourceTest extends WordSpec with Matchers {
  "JsonSource" should {
    "read multiple json docs from a file" in {
      val schema = StructType(Field("name"), Field("location"))
      implicit val fs = FileSystem.get(new Configuration())
      JsonSource(new Path(getClass.getResource("/test.json").getFile)).toFrame().toSet() shouldBe
        Set(
          Row(schema, "sammy", "aylesbury"),
          Row(schema, "ant", "greece")
        )
    }
  }
}