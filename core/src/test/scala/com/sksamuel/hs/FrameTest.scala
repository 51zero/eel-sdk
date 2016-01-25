package com.sksamuel.hs

import org.scalatest.{WordSpec, Matchers}

class FrameTest extends WordSpec with Matchers {

  "Frame" should {
    "support drop" in {
      Frame.fromSeq(Seq(Seq("a", "b"), Seq("c", "d"))).drop(1).size shouldBe 1
      Frame.fromSeq(Seq(Seq("a", "b"), Seq("c", "d"), Seq("e", "f"))).drop(1).size shouldBe 2
      Frame.fromSeq(Seq(Seq("a", "b"), Seq("c", "d"), Seq("e", "f"))).drop(2).size shouldBe 1
    }
  }
}
