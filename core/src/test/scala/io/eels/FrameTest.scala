package io.eels

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
      val f = frame.addColumn("testy", "bibble")
      f.head.get shouldBe Row(Seq("a", "b", "testy"), Seq("1", "2", "bibble"))
      f.schema shouldBe FrameSchema(List(Column("a"), Column("b"), Column("testy")))
    }
    "support removing columns" in {
      val columns = Seq(Column("a"), Column("b"), Column("c"), Column("d"))
      val frame = Frame(Row(columns, Seq("1", "2", "3", "4")), Row(columns, Seq("5", "6", "7", "8")))
      val f = frame.removeColumn("c")
      f.head.get shouldBe Row(Seq("a", "b", "d"), Seq("1", "2", "4"))
      f.schema shouldBe FrameSchema(List(Column("a"), Column("b"), Column("d")))
    }
    "support column projection" in {
      val columns = Seq(Column("a"), Column("b"), Column("c"), Column("d"))
      val frame = Frame(Row(columns, Seq("1", "2", "3", "4")), Row(columns, Seq("5", "6", "7", "8")))
      val f = frame.projection("d", "a", "c", "b")
      f.head.get shouldBe Row(Seq("d", "a", "c", "b"), Seq("4", "1", "3", "2"))
      f.schema shouldBe FrameSchema(Seq(Column("d"), Column("a"), Column("c"), Column("b")))
    }
    "support row filtering by column name and fn" in {
      frame.filter("b", _ == "2").size shouldBe 1
    }
    "support union" in {
      frame.union(frame).size shouldBe 4
      frame.union(frame).toList shouldBe List(
        Row(columns, Seq("1", "2")),
        Row(columns, Seq("3", "4")),
        Row(columns, Seq("1", "2")),
        Row(columns, Seq("3", "4"))
      )
    }
    "support collect" in {
      frame.collect {
        case row if row.fields.head.value == "3" => row
      }.size shouldBe 1
    }
    "support ++" in {
      frame.++(frame).size shouldBe 4
    }
    "support reduceLeft" in {
      frame.reduceLeft((a, b) => a).toList shouldBe List(Row(List("a", "b"), List("1", "2")))
    }
    "support joins" in {
      val frame1 = Frame(Row(Map("a" -> "sam", "b" -> "bam")))
      val frame2 = Frame(Row(Map("c" -> "ham", "d" -> "jam")))
      frame1.join(frame2).schema shouldBe FrameSchema(Seq(Column("a"), Column("b"), Column("c"), Column("d")))
      frame1.join(frame2).head.get shouldBe Row(Map("a" -> "sam", "b" -> "bam", "c" -> "ham", "d" -> "jam"))
    }
    "support multirow joins" in {
      val frame1 = Frame(Row(Map("a" -> "sam", "b" -> "bam")), Row(Map("a" -> "ham", "b" -> "jam")))
      val frame2 = Frame(Row(Map("c" -> "gary", "d" -> "harry")), Row(Map("c" -> "barry", "d" -> "larry")))
      frame1.join(frame2).schema shouldBe FrameSchema(Seq(Column("a"), Column("b"), Column("c"), Column("d")))
      frame1.join(frame2).toList shouldBe List(Row(Map("a" -> "sam", "b" -> "bam", "c" -> "gary", "d" -> "harry")), Row(Map("a" -> "ham", "b" -> "jam", "c" -> "barry", "d" -> "larry")))
    }
  }
}
