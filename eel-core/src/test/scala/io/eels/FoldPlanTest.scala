package io.eels

import org.scalatest.{Matchers, WordSpec}

class FoldPlanTest extends WordSpec with Matchers {

  "FoldPlan" should {
    "fold!" in {
      val frame = Frame(
        Seq("a", "b"),
        Seq("sam", "1"),
        Seq("sam", "2"),
        Seq("sam", "3")
      )
      frame.fold(0)((a, row) => a + row(1).toString.toInt) shouldBe 6
    }
  }
}
