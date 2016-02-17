package io.eels

import org.scalatest.{Matchers, WordSpec}

class FoldPlanTest extends WordSpec with Matchers {

  "FoldPlan" should {
    "fold!" in {
      val frame = Frame(
        Row(Map("name" -> "sam", "risk" -> "1")),
        Row(Map("name" -> "sam", "risk" -> "2")),
        Row(Map("name" -> "sam", "risk" -> "3"))
      )
      frame.fold(0)((a, row) => a + row("risk").toInt).run shouldBe 6
    }
  }
}
