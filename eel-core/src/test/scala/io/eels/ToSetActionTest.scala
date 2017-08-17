package io.eels

import io.eels.datastream.DataStream
import io.eels.schema._
import org.scalatest.{Matchers, WordSpec}

class ToSetActionTest extends WordSpec with Matchers {

  "ToSetPlan" should {
    "createReader set from frame" in {
      val schema = StructType(
        Field("name"),
        Field("location")
      )
      val ds = DataStream.fromValues(
        schema,
        Seq(
          List("sam", "aylesbury"),
          List("sam", "aylesbury"),
          List("sam", "aylesbury"),
          List("jam", "aylesbury"),
          List("jam", "aylesbury"),
          List("jam", "aylesbury"),
          List("ham", "buckingham")
        )
      )
      ds.toSet shouldBe Set(
        Row(ds.schema, "sam", "aylesbury"),
        Row(ds.schema, "jam", "aylesbury"),
        Row(ds.schema, "ham", "buckingham")
      )
    }
  }
}
