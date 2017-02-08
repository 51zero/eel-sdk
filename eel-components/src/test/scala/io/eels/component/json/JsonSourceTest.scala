package io.eels.component.json

import io.eels.Row
import io.eels.schema.{Field, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, WordSpec}

class JsonSourceTest extends WordSpec with Matchers {

  implicit val conf = new Configuration()
  implicit val fs: FileSystem = FileSystem.get(conf)

  "JsonSource" should {
    "read multiple json docs from a file" in {
      val schema = StructType(Field("name"), Field("location"))
      JsonSource(new Path(getClass.getResource("/io/eels/component/json/test.json").getFile)).toFrame().toSet() shouldBe
        Set(
          Row(schema, "sammy", "aylesbury"),
          Row(schema, "ant", "greece")
        )
    }
    "support maps" in {
      val schema = StructType(Field("name"), Field("location"), Field("skills"))
      JsonSource(new Path(getClass.getResource("/io/eels/component/json/maps.json").getFile)).toFrame().toSet() shouldBe
        Set(
          Row(schema, "sammy", "aylesbury", Map("karate" -> "black belt", "chess" -> "grandmaster", "100m" -> 9.23))
        )
    }
    "support arrays" in {
      val schema = StructType(Field("name"), Field("skills"))
      JsonSource(new Path(getClass.getResource("/io/eels/component/json/arrays.json").getFile)).toFrame().toSet() shouldBe
        Set(
          Row(schema, "sammy", Seq("karate", "chess", "running"))
        )
    }
  }
}