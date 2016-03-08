package io.eels

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, WordSpec}
import Frame._

case class Wibble(name: String, location: String, postcode: String)

class FrameTest extends WordSpec with Matchers with Eventually {

  import scala.concurrent.ExecutionContext.Implicits.global

  val frame = Frame(List("a", "b"), List("1", "2"), List("3", "4"))

  "Frame.addColumnIfNotExists" should {
    "not add column if already exists" in {
      val f = frame.addColumnIfNotExists("a", "bibble")
      f.schema shouldBe Schema(List(Column("a"), Column("b")))
      f.head.get shouldBe List("1", "2")
    }
    "add column if it does not exist" in {
      val f = frame.addColumnIfNotExists("testy", "bibble")
      f.schema shouldBe Schema(List(Column("a"), Column("b"), Column("testy")))
      f.head.get shouldBe List("1", "2", "bibble")
    }
  }

  "Frame.addColumn" should {
    "support adding columns" in {
      val f = frame.addColumn("testy", "bibble")
      f.schema shouldBe Schema(List(Column("a"), Column("b"), Column("testy")))
      f.head.get shouldBe List("1", "2", "bibble")
    }
  }

  "Frame.foreach" should {
    "execute for every row" in {
      val count = new AtomicInteger(0)
      frame.foreach(_ => count.incrementAndGet).size
      eventually {
        count.get() shouldBe 2
      }
    }
  }

  "Frame.stripFromColumnName" should {
    "remove offending characters" in {
      val frame = Frame(
        List("name", "#location", "!postcode"),
        List("sam", "aylesbury", "hp22"),
        List("ham", "buckingham", "mk10")
      )
      frame.stripFromColumnName(Seq('#', '!', 'p')).schema shouldBe
        Schema(List(Column("name"), Column("location"),Column("ostcode")))
    }
  }

  "Frame.removeColumn" should {
    "remove column" in {
      val frame = Frame(
        List("name", "location", "postcode"),
        List("sam", "aylesbury", "hp22"),
        List("ham", "buckingham", "mk10")
      )
      val f = frame.removeColumn("location")
      f.schema shouldBe Schema(List(Column("name"), Column("postcode")))
      f.toSeq.toSet shouldBe Set(Row(f.schema, "sam", "hp22"), Row(f.schema, "ham", "mk10"))
    }
    "not remove column if case is different" in {
      val frame = Frame(
        List("name", "location", "postcode"),
        List("sam", "aylesbury", "hp22"),
        List("ham", "buckingham", "mk10")
      )
      val f = frame.removeColumn("POSTcode")
      f.schema shouldBe Schema(List(Column("name"), Column("location"), Column("postcode")))
      f.toSeq.toSet shouldBe Set(Row(f.schema, "sam", "aylesbury", "hp22"), Row(f.schema, "ham", "buckingham", "mk10"))
    }
    "remove column with ignore case" in {
      val frame = Frame(
        List("name", "location", "postcode"),
        List("sam", "aylesbury", "hp22"),
        List("ham", "buckingham", "mk10")
      )
      val f = frame.removeColumn("locATION", false)
      f.schema shouldBe Schema(List(Column("name"), Column("postcode")))
      f.toSeq.toSet shouldBe Set(Row(f.schema, "sam", "hp22"), Row(f.schema, "ham", "mk10"))
    }
  }

  "Frame.toSet[T]" should {
    "marshall result into instances of T" in {
      val frame = Frame(
        List("name", "location", "postcode"),
        List("sam", "aylesbury", "hp22"),
        List("ham", "buckingham", "mk10")
      )
      frame.toSetAs[Wibble] shouldBe Set(Wibble("sam", "aylesbury", "hp22"), Wibble("ham", "buckingham", "mk10"))
    }
  }

  "Frame.toSeq[T]" should {
    "marshall result into instances of T" in {
      val frame = Frame(
        List("name", "location", "postcode"),
        List("ham", "buckingham", "mk10")
      )
      frame.toSeqAs[Wibble] shouldBe Seq(Wibble("ham", "buckingham", "mk10"))
    }
  }

  "Frame.filter" should {
    "support row filtering by column name and fn" in {
      frame.filter("b", _ == "2").size shouldBe 1
    }
    "throw if the column does not exist" in {
      intercept[RuntimeException] {
        frame.filter("qweeg", _ == "1").size
      }
    }
  }

  "Frame" should {
    "be immutable and repeatable" in {
      val f = frame.drop(1)
      f.drop(1).size shouldBe 0
      f.size shouldBe 1
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
      val frame = Frame(Schema(Seq("k")), rows)
      frame.drop(100).toSeq.size shouldBe 9900
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
      f.schema shouldBe Schema(List(Column("location")))
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
      f.schema shouldBe Schema(List(Column("location"), Column("name")))
    }
    "support column projection re-ordering" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      val f = frame.projection("location", "name")
      f.schema shouldBe Schema(List(Column("location"), Column("name")))
      f.head.get shouldBe List("aylesbury", "sam")
    }
    "support union" in {
      frame.union(frame).size shouldBe 4
      frame.union(frame).toSeq.toSet shouldBe Set(
        Row(frame.schema, "1", "2"),
        Row(frame.schema, "3", "4"),
        Row(frame.schema, "1", "2"),
        Row(frame.schema, "3", "4")
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
      frame1.join(frame2).schema shouldBe Schema(List(Column("a"), Column("b"), Column("c"), Column("d")))
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
      val schema = Schema(Seq("name"))
      frame1.except(frame2).schema shouldBe schema
      frame1.except(frame2).toSeq.toSet shouldBe Set(
        Row(schema, "sam"),
        Row(schema, "jam"),
        Row(schema, "ham")
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
        Row(frame.schema, "sam", "aylesbury"),
        Row(frame.schema, "jam", "aylesbury")
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
        Row(frame.schema, "sam", "aylesbury"),
        Row(frame.schema, "jam", "aylesbury")
      )
    }
    "support drop while" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.dropWhile(_.apply(1) == "aylesbury").toSeq shouldBe List(Row(frame.schema, "ham", "buckingham"))
    }
    "support drop while with column predicate" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.dropWhile("location", _ == "aylesbury").toSeq shouldBe List(
        Row(frame.schema, "ham", "buckingham")
      )
    }
    "support explode" in {
      val f = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      f.explode(row => Seq(row, row)).toSeq.toSet shouldBe {
        Set(
          Row(f.schema, "sam", "aylesbury"),
          Row(f.schema, "sam", "aylesbury"),
          Row(f.schema, "jam", "aylesbury"),
          Row(f.schema, "jam", "aylesbury"),
          Row(f.schema, "ham", "buckingham"),
          Row(f.schema, "ham", "buckingham")
        )
      }
    }
    "support fill" in {
      val f = Frame(
        List("name", "location"),
        List("sam", null),
        List("jam", "aylesbury"),
        List(null, "buckingham")
      )
      f.fill("foo").toSeq.toSet shouldBe {
        Set(
          Row(f.schema, "sam", "foo"),
          Row(f.schema, "jam", "aylesbury"),
          Row(f.schema, "foo", "buckingham")
        )
      }
    }
    "throw an error if the column is not present while filtering" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("ham", "buckingham")
      )
      intercept[IllegalArgumentException] {
        frame.filter("bibble", v => sys.error("Should not be here")).toSeq
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
          Row(frame.schema, "ham", "aylesbury"),
          Row(frame.schema, "ham", "buckingham")
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
          Row(frame.schema, "ham", "aylesbury"),
          Row(frame.schema, "ham", "buckingham")
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
          Row(frame.schema, "mas", "aylesbury"),
          Row(frame.schema, "mah", "buckingham")
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
          Row(frame.schema, "ham", "buckingham")
        )
      }
    }
    "support column update" in {
      val frame = Frame(
        Schema("name", "location"),
        List("sam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.updateColumn(Column("name", SchemaType.Int, true)).schema shouldBe Schema(Column("name", SchemaType.Int, true), Column("location", SchemaType.String, false))
    }
    "support column rename" in {
      val frame = Frame(
        List("name", "location"),
        List("sam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.renameColumn("name", "blame").schema shouldBe Schema("blame", "location")
    }
    "convert from a Seq[T<:Product]" ignore {
      val p1 = PersonA("name1", 2, 1.2, true, 11, 3, 1)
      val p2 = PersonA("name2", 3, 11.2, true, 11111, 3121, 436541)
      val seq = Seq(p1, p2)

      val frame: Frame = seq

      val rows = frame.toSeq
      rows.size shouldBe 2
      rows shouldBe Seq(
        Row(frame.schema, Seq("name1", 2, 1.2, true, 11, 3, 1)),
        Row(frame.schema, Seq("name2", 3, 11.2, true, 11111, 3121, 436541))
      )
    }
  }

  case class PersonA(name: String, age: Int, salary: Double, isPartTime: Boolean, value1: BigDecimal, value2: Float, value3: Long) extends StrictLogging

}
