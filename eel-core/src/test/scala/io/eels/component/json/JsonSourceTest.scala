package io.eels.component.json

import io.eels.Row
import io.eels.schema.{ArrayType, Field, StringType, StructType}
import org.scalatest.{Matchers, WordSpec}

class JsonSourceTest extends WordSpec with Matchers {

  "JsonSource" should {
    "read multiple json docs from a file" in {
      val schema = StructType(Field("name"), Field("location"))
      JsonSource(() => getClass.getResourceAsStream("/io/eels/component/json/test.json")).toDataStream().toSet shouldBe
        Set(
          Row(schema, "sammy", "aylesbury"),
          Row(schema, "ant", "greece")
        )
    }
    "return schema for nested fields" in {
      JsonSource(() => getClass.getResourceAsStream("/io/eels/component/json/nested.json")).schema shouldBe
        StructType(
          Field("name", StringType),
          Field("alias", StringType),
          Field("friends", ArrayType(
            StructType(
              Field("name", StringType),
              Field("location", StringType)
            )
          ))
        )
    }
    "support all primitives" in {
      val schema = StructType(Field("int"), Field("double"), Field("long"), Field("boolean"))
      JsonSource(() => getClass.getResourceAsStream("/io/eels/component/json/prims.json")).toDataStream().toSet shouldBe
        Set(
          Row(schema, 145342, 369.235195, 10151589328923L, true)
        )
    }
    "support maps" in {
      val schema = StructType(Field("name"), Field("location"), Field("skills", StructType(Field("karate"), Field("chess"), Field("100m"))))
      JsonSource(() => getClass.getResourceAsStream("/io/eels/component/json/maps.json")).toDataStream().toSet shouldBe
        Set(
          Row(schema, Seq("sammy", "aylesbury", Map("karate" -> "black belt", "chess" -> "grandmaster", "100m" -> 9.23)))
        )
    }
    "support arrays" in {
      val schema = StructType(Field("name"), Field("skills", ArrayType(StringType)))
      JsonSource(() => getClass.getResourceAsStream("/io/eels/component/json/arrays.json")).toDataStream().toSet shouldBe
        Set(
          Row(schema, Seq("sammy", Seq("karate", "chess", "running")))
        )
    }
  }
}