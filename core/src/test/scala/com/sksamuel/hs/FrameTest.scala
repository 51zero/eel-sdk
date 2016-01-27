package com.sksamuel.hs

import com.sksamuel.hs.sink.{Column, Row}
import org.scalatest.{Matchers, WordSpec}

class FrameTest extends WordSpec with Matchers {

  val columns = Seq(Column("a"), Column("b"))
  val frame = Frame(Row(columns, Seq("1", "2")), Row(columns, Seq("3", "4")))

  "Frame" should {
    "be immutable and repeatable" in {
      val f = frame.drop(1)
      f.drop(1).size shouldBe 0
      f.size shouldBe 1
    }
    "support foreach" in {
      var count = 0
      val f = frame.foreach(_ => count = count + 1)
      f.size
      count shouldBe 2
    }
    "support forall" in {
      frame.forall(_.size == 1) shouldBe false
      frame.forall(_.size == 2) shouldBe true
    }
    "support exists" in {
      frame.exists(_.size == 1) shouldBe false
      frame.exists(_.size == 2) shouldBe true
    }
    "support drop" in {
      frame.drop(1).size shouldBe 1
      frame.drop(0).size shouldBe 2
      frame.drop(2).size shouldBe 0
    }
    "support adding columns" in {
      frame.addColumn("testy", "bibble").head.get shouldBe Row(Seq("a", "b", "testy"), Seq("1", "2", "bibble"))
    }
    "support removing columns" in {
      val columns = Seq(Column("a"), Column("b"), Column("c"), Column("d"))
      val frame = Frame(Row(columns, Seq("1", "2", "3", "4")), Row(columns, Seq("5", "6", "7", "8")))
      frame.removeColumn("c").head.get shouldBe Row(Seq("a", "b", "d"), Seq("1", "2", "4"))
    }
    "support column projection" in {
      val columns = Seq(Column("a"), Column("b"), Column("c"), Column("d"))
      val frame = Frame(Row(columns, Seq("1", "2", "3", "4")), Row(columns, Seq("5", "6", "7", "8")))
      frame.projection("d", "a", "c", "b").head.get shouldBe Row(Seq("d", "a", "c", "b"), Seq("4", "1", "3", "2"))
    }
    "support row filtering by column name and fn" in {
      frame.filter("b", _ == "2").size shouldBe 1
    }
  }
}
