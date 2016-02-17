package io.eels

import org.scalatest.{Matchers, WordSpec}

class ToSetPlanTest extends WordSpec with Matchers {

  "ToSetPlan" should {
    "create set from frame" in {
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
      frame.toSet.run shouldBe Set(
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
    }
  }
}
