package io.eels

import org.scalatest.{Matchers, WordSpec}

class FrameTest extends WordSpec with Matchers {

  val columns = List(Column("a"), Column("b"))
  val frame = Frame(Row(columns, List("1", "2")), Row(columns, List("3", "4")))

  "Frame" should {
    "be immutable and repeatable" in {
      val f = frame.drop(1)
      f.drop(1).size.run shouldBe 0
      f.size.run shouldBe 1
    }
    "support foreach" in {
      var count = 0
      val f = frame.foreach(_ => count = count + 1)
      f.size.run
      count shouldBe 2
    }
    "support forall" in {
      frame.forall(_.size == 1).run shouldBe false
      frame.forall(_.size == 2).run shouldBe true
    }
    "support exists" in {
      frame.exists(_.size == 1).run shouldBe false
      frame.exists(_.size == 2).run shouldBe true
    }
    "support drop" in {
      frame.drop(1).size.run shouldBe 1
      frame.drop(0).size.run shouldBe 2
      frame.drop(2).size.run shouldBe 0
    }
    "be thread safe when using drop" in {
      val rows = Iterator.tabulate(10000)(k => Row(Map("k" -> k.toString))).toList
      val frame = Frame(rows)
      frame.drop(100).toList.runConcurrent(4).size shouldBe 9900
    }
    "support adding columns" in {
      val f = frame.addColumn("testy", "bibble")
      f.head.run.get shouldBe Row(List("a", "b", "testy"), List("1", "2", "bibble"))
      f.schema shouldBe FrameSchema(List(Column("a"), Column("b"), Column("testy")))
    }
    "support removing columns" in {
      val columns = List(Column("a"), Column("b"), Column("c"), Column("d"))
      val frame = Frame(Row(columns, List("1", "2", "3", "4")), Row(columns, List("5", "6", "7", "8")))
      val f = frame.removeColumn("c")
      f.head.run.get shouldBe Row(List("a", "b", "d"), List("1", "2", "4"))
      f.schema shouldBe FrameSchema(List(Column("a"), Column("b"), Column("d")))
    }
    "support column projection" in {
      val columns = List(Column("a"), Column("b"), Column("c"), Column("d"))
      val frame = Frame(Row(columns, List("1", "2", "3", "4")), Row(columns, List("5", "6", "7", "8")))
      val f = frame.projection("d", "a", "c", "b")
      f.head.run.get shouldBe Row(List("d", "a", "c", "b"), List("4", "1", "3", "2"))
      f.schema shouldBe FrameSchema(List(Column("d"), Column("a"), Column("c"), Column("b")))
    }
    "support row filtering by column name and fn" in {
      frame.filter("b", _ == "2").size.run shouldBe 1
    }
    "support union" in {
      frame.union(frame).size.run shouldBe 4
      frame.union(frame).toList.run shouldBe List(
        Row(columns, List("1", "2")),
        Row(columns, List("3", "4")),
        Row(columns, List("1", "2")),
        Row(columns, List("3", "4"))
      )
    }
    "support collect" in {
      frame.collect {
        case row if row.fields.head.value == "3" => row
      }.size.run shouldBe 1
    }
    "support ++" in {
      frame.++(frame).size.run shouldBe 4
    }
    "support joins" in {
      val frame1 = Frame(Row(Map("a" -> "sam", "b" -> "bam")))
      val frame2 = Frame(Row(Map("c" -> "ham", "d" -> "jam")))
      frame1.join(frame2).schema shouldBe FrameSchema(List(Column("a"), Column("b"), Column("c"), Column("d")))
      frame1.join(frame2).head.run.get shouldBe Row(Map("a" -> "sam", "b" -> "bam", "c" -> "ham", "d" -> "jam"))
    }
    "support multirow joins" in {
      val frame1 = Frame(Row(Map("a" -> "sam", "b" -> "bam")), Row(Map("a" -> "ham", "b" -> "jam")))
      val frame2 = Frame(Row(Map("c" -> "gary", "d" -> "harry")), Row(Map("c" -> "barry", "d" -> "larry")))
      frame1.join(frame2).schema shouldBe FrameSchema(List(Column("a"), Column("b"), Column("c"), Column("d")))
      frame1.join(frame2).toList.run shouldBe List(Row(Map("a" -> "sam", "b" -> "bam", "c" -> "gary", "d" -> "harry")), Row(Map("a" -> "ham", "b" -> "jam", "c" -> "barry", "d" -> "larry")))
    }
    "support except" in {
      val frame1 = Frame(Row(Map("name" -> "sam", "location" -> "aylesbury")))
      val frame2 = Frame(Row(Map("landmark" -> "st pauls", "location" -> "london")))
      frame1.except(frame2).toList.run shouldBe List(Row(Map("name" -> "sam")))
    }
    "support take while with row predicate" in {
      val frame = Frame(
        Row(Map("name" -> "sam", "location" -> "aylesbury")),
        Row(Map("name" -> "jam", "location" -> "aylesbury")),
        Row(Map("name" -> "ham", "location" -> "buckingham"))
      )
      frame.takeWhile(_.apply("location") == "aylesbury").toList.run shouldBe List(
        Row(Map("name" -> "sam", "location" -> "aylesbury")),
        Row(Map("name" -> "jam", "location" -> "aylesbury"))
      )
    }
    "support take while with column predicate" in {
      val frame = Frame(
        Row(Map("name" -> "sam", "location" -> "aylesbury")),
        Row(Map("name" -> "jam", "location" -> "aylesbury")),
        Row(Map("name" -> "ham", "location" -> "buckingham"))
      )
      frame.takeWhile("location", _ == "aylesbury").toList.run shouldBe List(
        Row(Map("name" -> "sam", "location" -> "aylesbury")),
        Row(Map("name" -> "jam", "location" -> "aylesbury"))
      )
    }
    "support drop while" in {
      val frame = Frame(
        Row(Map("name" -> "sam", "location" -> "aylesbury")),
        Row(Map("name" -> "jam", "location" -> "aylesbury")),
        Row(Map("name" -> "ham", "location" -> "buckingham"))
      )
      frame.dropWhile(_.apply("location") == "aylesbury").toList.run shouldBe List(
        Row(Map("name" -> "ham", "location" -> "buckingham"))
      )
    }
    "support drop while with column predicate" in {
      val frame = Frame(
        Row(Map("name" -> "sam", "location" -> "aylesbury")),
        Row(Map("name" -> "jam", "location" -> "aylesbury")),
        Row(Map("name" -> "ham", "location" -> "buckingham"))
      )
      frame.dropWhile("location", _ == "aylesbury").toList.run shouldBe List(
        Row(Map("name" -> "ham", "location" -> "buckingham"))
      )
    }
  }
}
