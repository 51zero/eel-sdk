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
          Vector("sam", "aylesbury"),
          Vector("sam", "aylesbury"),
          Vector("sam", "aylesbury"),
          Vector("jam", "aylesbury"),
          Vector("jam", "aylesbury"),
          Vector("jam", "aylesbury"),
          Vector("ham", "buckingham")
        )
      )
      ds.toSet shouldBe Set(
        Vector("sam", "aylesbury"),
        Vector("jam", "aylesbury"),
        Vector("ham", "buckingham")
      )
    }
  }
}
