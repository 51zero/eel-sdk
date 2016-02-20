package io.eels

import org.scalatest.{Matchers, WordSpec}

class FoldPlanTest extends WordSpec with Matchers {

  "FoldPlan" should {
    "fold!" in {
      val frame = Frame(
        Map("name" -> "sam", "risk" -> "1"),
        Map("name" -> "sam", "risk" -> "2"),
        Map("name" -> "sam", "risk" -> "3")
      )
      frame.fold(0)((a, row) => a + row(1).toString.toInt) shouldBe 6
    }
  }
}
