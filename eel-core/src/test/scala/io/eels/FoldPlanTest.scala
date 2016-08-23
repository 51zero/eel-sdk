package io.eels

import io.eels.schema.Schema
import org.scalatest.{Matchers, WordSpec}

class FoldPlanTest extends WordSpec with Matchers {

  "FoldPlan" should {
    "fold!" in {
      val frame = Frame.fromValues(
        Schema("a", "b"),
        Seq("sam", "1"),
        Seq("sam", "2"),
        Seq("sam", "3")
      )
      frame.fold(0) { case (a, row) => a + row.get(1).toString.toInt } shouldBe 6
    }
  }
}
