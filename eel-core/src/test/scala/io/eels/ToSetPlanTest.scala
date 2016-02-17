package io.eels

import org.scalatest.{WordSpec, Matchers}

class ToSetPlanTest extends WordSpec with Matchers {

  "ToSetPlan" should {
    "create set from frame" in {
      val frame = Frame(
        Row(Map("name" -> "sam", "location" -> "aylesbury")),
        Row(Map("name" -> "sam", "location" -> "aylesbury")),
        Row(Map("name" -> "sam", "location" -> "buckingham"))
      )
      frame.toSet.run shouldBe Set(
        Row(Map("name" -> "sam", "location" -> "aylesbury")),
        Row(Map("name" -> "sam", "location" -> "buckingham"))
      )
    }
  }
}
