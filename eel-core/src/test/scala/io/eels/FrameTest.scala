package io.eels

import org.scalatest.{Matchers, WordSpec}

class FrameTest extends WordSpec with Matchers {

  val frame = Frame(List("a", "b"), List("1", "2"), List("3", "4"))

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
      val rows = Iterator.tabulate(10000)(k => Seq("1")).toList
      val frame = Frame(FrameSchema(Seq("k")), rows)
      frame.drop(100).toSeq.runConcurrent(4).size shouldBe 9900
    }
    "support adding columns" in {
      val f = frame.addColumn("testy", "bibble")
      f.head.run.get shouldBe List("1", "2", "bibble")
      f.schema shouldBe FrameSchema(List(Column("a"), Column("b"), Column("testy")))
    }
    "support removing columns" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      val f = frame.removeColumn("name")
      f.toSeq.run shouldBe List(List("aylesbury"), List("aylesbury"), List("buckingham"))
      f.schema shouldBe FrameSchema(List(Column("location")))
    }
    "support column projection" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      val f = frame.projection("location")
      f.head.run.get shouldBe Seq("aylesbury")
      f.schema shouldBe FrameSchema(List(Column("location")))
    }
    "support column projection re-ordering" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      val f = frame.projection("location", "name")
      f.schema shouldBe FrameSchema(List(Column("location"), Column("name")))
      f.head.run.get shouldBe List("aylesbury", "sam")
    }
    "support row filtering by column name and fn" in {
      frame.filter("b", _ == "2").size.run shouldBe 1
    }
    "support union" in {
      frame.union(frame).size.run shouldBe 4
      frame.union(frame).toSeq.run shouldBe List(
        List("1", "2"),
        List("3", "4"),
        List("1", "2"),
        List("3", "4")
      )
    }
    "support collect" in {
      frame.collect {
        case row if row.head == "3" => row
      }.size.run shouldBe 1
    }
    "support ++" in {
      frame.++(frame).size.run shouldBe 4
    }
    "support joins" in {
      val frame1 = Frame(List("a", "b"), List("sam", "bam"))
      val frame2 = Frame(List("c", "d"), List("ham", "jam"))
      frame1.join(frame2).schema shouldBe FrameSchema(List(Column("a"), Column("b"), Column("c"), Column("d")))
      frame1.join(frame2).head.run.get shouldBe List("sam", "bam", "ham", "jam")
    }
    "support except" in {
      val frame1 = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      val frame2 = Frame(
        List("landmark", "location"),
        List("x", "aylesbury"),
        List("y", "aylesbury"),
        List("z", "buckingham")
      )
      frame1.except(frame2).schema shouldBe FrameSchema(Seq("name"))
      frame1.except(frame2).toSeq.run shouldBe List(
        List("sam"),
        List("jam"),
        List("ham")
      )
    }
    "support take while with row predicate" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.takeWhile(_.apply(1) == "aylesbury").toSeq.run shouldBe List(
        List("sam", "aylesbury"),
        List("jam", "aylesbury")
      )
    }
    "support take while with column predicate" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.takeWhile("location", _ == "aylesbury").toSeq.run shouldBe List(
        List("sam", "aylesbury"),
        List("jam", "aylesbury")
      )
    }
    "support drop while" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.dropWhile(_.apply(1) == "aylesbury").toSeq.run shouldBe List(List("ham", "buckingham"))
    }
    "support drop while with column predicate" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.dropWhile("location", _ == "aylesbury").toSeq.run shouldBe List(
        List("ham", "buckingham")
      )
    }
    "support explode" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.explode(row => Seq(row, row)).toSeq.run shouldBe {
        List(
          List("sam", "aylesbury"),
          List("sam", "aylesbury"),
          List("jam", "aylesbury"),
          List("jam", "aylesbury"),
          List("ham", "buckingham"),
          List("ham", "buckingham")
        )
      }
    }
    "support fill" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", null),
        List("jam", "aylesbury"),
        List(null, "buckingham")
      )
      frame.fill("foo").toSeq.run shouldBe {
        List(
          List("sam", "foo"),
          List("jam", "aylesbury"),
          List("foo", "buckingham")
        )
      }
    }
    "support replace" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.replace("sam", "ham").toSeq.run shouldBe {
        List(
          List("ham", "aylesbury"),
          List("ham", "buckingham")
        )
      }
    }
    "support replace by column" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.replace("name", "sam", "ham").toSeq.run shouldBe {
        List(
          List("ham", "aylesbury"),
          List("ham", "buckingham")
        )
      }
    }
    "support replace by column with function" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("ham", "buckingham")
      )
      val fn: Any => Any = any => any.toString.reverse
      frame.replace("name", fn).toSeq.run shouldBe {
        List(
          List("mas", "aylesbury"),
          List("mah", "buckingham")
        )
      }
    }
    "support dropNullRows" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", null),
        List("ham", "buckingham")
      )
      frame.dropNullRows.toSeq.run shouldBe {
        List(
          List("ham", "buckingham")
        )
      }
    }
  }
}
