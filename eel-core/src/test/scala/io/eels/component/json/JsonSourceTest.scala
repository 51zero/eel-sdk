package io.eels.component.json

import io.eels.schema.{ArrayType, Field, StringType, StructType}
import org.scalatest.{Matchers, WordSpec}

class JsonSourceTest extends WordSpec with Matchers {

  "JsonSource" should {
    "read multiple json docs from a file" in {
      val schema = StructType(Field("name"), Field("location"))
      val ds = JsonSource(() => getClass.getResourceAsStream("/io/eels/component/json/test.json")).toDataStream()
      ds.schema shouldBe schema
      ds.toSet shouldBe
        Set(
          Vector("sammy", "aylesbury"),
          Vector("ant", "greece")
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
      val ds = JsonSource(() => getClass.getResourceAsStream("/io/eels/component/json/prims.json")).toDataStream()
      ds.schema shouldBe schema
      ds.toSet shouldBe
        Set(
          Vector(145342, 369.235195, 10151589328923L, true)
        )
    }
    "support maps" in {
      val schema = StructType(Field("name"), Field("location"), Field("skills", StructType(Field("karate"), Field("chess"), Field("100m"))))
      val ds = JsonSource(() => getClass.getResourceAsStream("/io/eels/component/json/maps.json")).toDataStream()
      ds.schema shouldBe schema
      ds.toSet shouldBe
        Set(
          Vector("sammy", "aylesbury", Map("karate" -> "black belt", "chess" -> "grandmaster", "100m" -> 9.23))
        )
    }
    "support arrays" in {
      val schema = StructType(Field("name"), Field("skills", ArrayType(StringType)))
      val ds = JsonSource(() => getClass.getResourceAsStream("/io/eels/component/json/arrays.json")).toDataStream()
      ds.schema shouldBe schema
      ds.toSet shouldBe
        Set(
          Vector("sammy", Seq("karate", "chess", "running"))
        )
    }
  }
}