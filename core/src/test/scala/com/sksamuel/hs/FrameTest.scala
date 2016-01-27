package com.sksamuel.hs

import com.sksamuel.hs.sink.{Column, Row}
import org.scalatest.{Matchers, WordSpec}

class FrameTest extends WordSpec with Matchers {

  val columns = Seq(Column("a"), Column("b"))
  val frame: Frame = Frame(Row(columns, Seq("1", "2")), Row(columns, Seq("3", "4")))

  "Frame" should {
    "be immutable and repeatable" in {
      val f = frame.drop(1)
      f.drop(2).size shouldBe 0
      f.drop(1).size shouldBe 0
      f.size shouldBe 1
    }
    "support drop" in {
      frame.drop(1).size shouldBe 1
      frame.drop(0).size shouldBe 2
      frame.drop(2).size shouldBe 0
    }
  }
}
