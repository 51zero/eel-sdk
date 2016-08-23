package io.eels

import io.eels.schema._
import org.scalatest.{Matchers, WordSpec}

class ToSetPlanTest extends WordSpec with Matchers {

  "ToSetPlan" should {
    "createReader set from frame" in {
      val schema = Schema(
        Field("name"),
        Field("location")
      )
      val frame = Frame.fromValues(
        schema,
        List("sam", "aylesbury"),
        List("sam", "aylesbury"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("jam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.toSet shouldBe Set(
        Row(frame.schema, "sam", "aylesbury"),
        Row(frame.schema, "jam", "aylesbury"),
        Row(frame.schema, "ham", "buckingham")
      )
    }
  }
}
