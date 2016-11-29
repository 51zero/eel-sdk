package io.eels

import io.eels.schema.StructType
import org.scalatest.{Matchers, WordSpec}

class FoldActionTest extends WordSpec with Matchers {

  "FoldPlan" should {
    "fold!" in {
      val frame = Frame.fromValues(
        StructType("a", "b"),
        Seq("sam", "1"),
        Seq("sam", "2"),
        Seq("sam", "3")
      )
      frame.fold(0) { case (a, row) => a + row.get(1).toString.toInt } shouldBe 6
    }
  }
}
