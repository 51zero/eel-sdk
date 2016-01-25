package com.sksamuel.hs

import com.sksamuel.hs.sink.{Column, Row}
import org.scalatest.{Matchers, WordSpec}

class FrameTest extends WordSpec with Matchers {

  val columns = Seq(Column("a"), Column("b"))

  "Frame" should {
    "support drop" in {
      Frame(Row(columns, Seq("1", "2")), Row(columns, Seq("3", "4"))).drop(1).size shouldBe 1
      Frame(Row(columns, Seq("1", "2")), Row(columns, Seq("3", "4"))).drop(0).size shouldBe 2
      Frame(Row(columns, Seq("1", "2")), Row(columns, Seq("3", "4"))).drop(2).size shouldBe 0
    }
  }
}
