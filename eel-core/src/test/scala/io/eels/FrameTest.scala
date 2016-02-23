package io.eels

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, WordSpec}
import Frame._

class FrameTest extends WordSpec with Matchers with Eventually {

  import scala.concurrent.ExecutionContext.Implicits.global

  val frame = Frame(List("a", "b"), List("1", "2"), List("3", "4"))

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
      eventually {
        count shouldBe 2
      }
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
    "be thread safe when using drop" in {
      val rows = Iterator.tabulate(10000)(k => Seq("1")).toList
      val frame = Frame(FrameSchema(Seq("k")), rows)
      frame.drop(100).toSeq.size shouldBe 9900
    }
    "support adding columns" in {
      val f = frame.addColumn("testy", "bibble")
      f.head.get shouldBe List("1", "2", "bibble")
      f.schema shouldBe FrameSchema(List(Column("a"), Column("b"), Column("testy")))
    }
    "support removing columns" in {
      val frame = Frame(
        List("name", "location", "postcode"),
        List("sam", "aylesbury", "hp22"),
        List("ham", "buckingham", "mk10")
      )
      val f = frame.removeColumn("location")
      f.schema shouldBe FrameSchema(List(Column("name"), Column("postcode")))
      f.toSeq.toSet shouldBe Set(Seq("sam", "hp22"), Seq("ham", "mk10"))
    }
    "support column projection" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      val f = frame.projection("location")
      f.head.get shouldBe Seq("aylesbury")
      f.schema shouldBe FrameSchema(List(Column("location")))
    }
    "support column projection expressions" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      val f = frame.projectionExpression("location,name")
      f.head.get shouldBe Seq("aylesbury", "sam")
      f.schema shouldBe FrameSchema(List(Column("location"), Column("name")))
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
      f.head.get shouldBe List("aylesbury", "sam")
    }
    "support row filtering by column name and fn" in {
      frame.filter("b", _ == "2").size shouldBe 1
    }
    "support union" in {
      frame.union(frame).size shouldBe 4
      frame.union(frame).toSeq.toSet shouldBe Set(
        List("1", "2"),
        List("3", "4"),
        List("1", "2"),
        List("3", "4")
      )
    }
    "support collect" in {
      frame.collect {
        case row if row.head == "3" => row
      }.size shouldBe 1
    }
    "support ++" in {
      frame.++(frame).size shouldBe 4
    }
    "support joins" in {
      val frame1 = Frame(List("a", "b"), List("sam", "bam"))
      val frame2 = Frame(List("c", "d"), List("ham", "jam"))
      frame1.join(frame2).schema shouldBe FrameSchema(List(Column("a"), Column("b"), Column("c"), Column("d")))
      frame1.join(frame2).head.get shouldBe List("sam", "bam", "ham", "jam")
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
      frame1.except(frame2).toSeq.toSet shouldBe Set(
        Seq("sam"),
        Seq("jam"),
        Seq("ham")
      )
    }
    "support take while with row predicate" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.takeWhile(_.apply(1) == "aylesbury").toSeq.toSet shouldBe Set(
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
      frame.takeWhile("location", _ == "aylesbury").toSeq.toSet shouldBe Set(
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
      frame.dropWhile(_.apply(1) == "aylesbury").toSeq shouldBe List(List("ham", "buckingham"))
    }
    "support drop while with column predicate" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.dropWhile("location", _ == "aylesbury").toSeq shouldBe List(
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
      frame.explode(row => Seq(row, row)).toSeq.toSet shouldBe {
        Set(
          Seq("sam", "aylesbury"),
          Seq("sam", "aylesbury"),
          Seq("jam", "aylesbury"),
          Seq("jam", "aylesbury"),
          Seq("ham", "buckingham"),
          Seq("ham", "buckingham")
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
      frame.fill("foo").toSeq.toSet shouldBe {
        Set(
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
      frame.replace("sam", "ham").toSeq.toSet shouldBe {
        Set(
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
      frame.replace("name", "sam", "ham").toSeq.toSet shouldBe {
        Set(
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
      frame.replace("name", fn).toSeq.toSet shouldBe {
        Set(
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
      frame.dropNullRows.toSeq shouldBe {
        List(
          List("ham", "buckingham")
        )
      }
    }
    "convert from a Seq[T<:Product]" in {
      val p1 = PersonA("name1", 2, 1.2, true, 11, 3, 1)
      val p2 = PersonA("name2", 3, 11.2, true, 11111, 3121, 436541)
      val seq = Seq(p1, p2)

      val rows = Frame.from(seq).toSeq
      rows.size shouldBe 2
      rows shouldBe Seq(
        Seq("name1", 2, 1.2, true, 11, 3, 1),
        Seq("name2", 3, 11.2, true, 11111, 3121, 436541)
      )
      var row = rows.head
      row(0) shouldBe p1.name
      row(1) shouldBe p1.age
      row(2) shouldBe p1.salary
      row(3) shouldBe p1.isPartTime
      row(4) shouldBe p1.value1
      row(5) shouldBe p1.value2
      row(6) shouldBe p1.value3


      row = rows.tail.head
      row(0) shouldBe p2.name
      row(1) shouldBe p2.age
      row(2) shouldBe p2.salary
      row(3) shouldBe p2.isPartTime
      row(4) shouldBe p2.value1
      row(5) shouldBe p2.value2
      row(6) shouldBe p2.value3
    }


  }

  case class PersonA(name: String, age: Int, salary: Double, isPartTime: Boolean, value1: BigDecimal, value2: Float, value3: Long) extends StrictLogging

}
