package io.eels

import org.scalatest.{Matchers, WordSpec}

class ToSetPlanTest extends WordSpec with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  "ToSetPlan" should {
    "createReader set from frame" in {
      val frame = Frame(
        List("name", "location"),
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
