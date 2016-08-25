package io.eels

import java.util.concurrent.atomic.AtomicInteger

import io.eels.schema.{Field, FieldType, Schema}
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, WordSpec}

case class Wibble(name: String, location: String, postcode: String)

class FrameTest extends WordSpec with Matchers with Eventually {

  val schema = Schema("a", "b")
  val frame = Frame.fromValues(schema, Vector("1", "2"), Vector("3", "4"))

  "Frame.withLowerCaseSchema" should {
    "return same values but with lower case schema" in {
      val schema = Schema("A", "B", "c")
      val f = Frame.fromValues(schema, Vector("x", "Y", null)).withLowerCaseSchema()
      f.schema() shouldBe Schema("a", "b", "c")
      f.toList() shouldBe List(Row(f.schema(), Vector("x", "Y", null)))
    }
  }

  "Frame.addFieldIfNotExists" should {
    "not add column if already exists" in {
      val f = frame.addFieldIfNotExists("a", "bibble")
      f.schema() shouldBe schema
      f.toList() shouldBe List(Row(schema, Vector("1", "2")), Row(schema, Vector("3", "4")))
    }
    "add column if it does not exist" in {
      val f = frame.addFieldIfNotExists("testy", "bibble")
      f.schema shouldBe Schema("a", "b", "testy")
      f.toList() shouldBe List(Row(schema.addFieldIfNotExists("testy"), Vector("1", "2", "bibble")), Row(schema.addFieldIfNotExists("testy"), Vector("3", "4", "bibble")))
    }
  }

  "Frame.addField" should {
    "support adding columns" in {
      val f = frame.addField("testy", "bibble")
      f.schema shouldBe Schema("a", "b", "testy")
      f.head.values shouldBe Vector("1", "2", "bibble")
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

  "Frame.stripFromFieldNames" should {
    "remove offending characters" in {
      val frame = Frame.fromValues(
        Schema("name", "#location", "!postcode"),
        List("sam", "aylesbury", "hp22"),
        List("ham", "buckingham", "mk10")
      )
      frame.stripCharsFromFieldNames(Seq('#', '!', 'p')).schema shouldBe
        Schema("name", "location", "ostcode")
    }
  }

  "Frame.removeField" should {
    "remove column" in {
      val frame = Frame.fromValues(
        Schema("name", "location", "postcode"),
        List("sam", "aylesbury", "hp22"),
        List("ham", "buckingham", "mk10")
      )
      val f = frame.removeField("location")
      f.schema shouldBe Schema("name", "postcode")
      f.toSet shouldBe Set(Row(f.schema, "sam", "hp22"), Row(f.schema, "ham", "mk10"))
    }
    "not remove column if case is different" in {
      val frame = Frame.fromValues(
        Schema("name", "location", "postcode"),
        List("sam", "aylesbury", "hp22"),
        List("ham", "buckingham", "mk10")
      )
      val f = frame.removeField("POSTcode")
      f.schema shouldBe Schema("name", "location", "postcode")
      f.toSet shouldBe Set(Row(f.schema, "sam", "aylesbury", "hp22"), Row(f.schema, "ham", "buckingham", "mk10"))
    }
    "remove column with ignore case" in {
      val frame = Frame.fromValues(
        Schema("name", "location", "postcode"),
        List("sam", "aylesbury", "hp22"),
        List("ham", "buckingham", "mk10")
      )
      val f = frame.removeField("locATION", false)
      f.schema shouldBe Schema("name", "postcode")
      f.toSet shouldBe Set(Row(f.schema, "sam", "hp22"), Row(f.schema, "ham", "mk10"))
    }
  }

//  "Frame.toSet[T]" should {
//    "marshall result into instances of T" in {
//      val frame = Frame(
//        Schema("name", "location", "postcode"),
//        List("sam", "aylesbury", "hp22"),
//        List("ham", "buckingham", "mk10")
//      )
//      frame.toSetAs[Wibble] shouldBe Set(Wibble("sam", "aylesbury", "hp22"), Wibble("ham", "buckingham", "mk10"))
//    }
//  }
//
//  "Frame.toSeq[T]" should {
//    "marshall result into instances of T" in {
//      val frame = Frame(
//        Schema("name", "location", "postcode"),
//        List("ham", "buckingham", "mk10")
//      )
//      frame.toSeqAs[Wibble] shouldBe Seq(Wibble("ham", "buckingham", "mk10"))
//    }
//  }

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

  "Frame.join" should {
    "cat two frames" in {
      val frame1 = Frame.fromValues(Schema("a", "b"), List("sam", "bam"))
      val frame2 = Frame.fromValues(Schema("c", "d"), List("ham", "jam"))
      frame1.join(frame2).schema shouldBe Schema("a", "b", "c", "d")
      frame1.join(frame2).head.values shouldBe Vector("sam", "bam", "ham", "jam")
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
      val schema = Schema("k")
      val rows = Iterator.tabulate(10000)(k => Row(schema, Vector("1"))).toList
      val frame = Frame(schema, rows)
      frame.drop(100).size shouldBe 9900
    }
    "support column projection" in {
      val frame = Frame.fromValues(
        Schema("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      val f = frame.projection("location")
      f.head.values shouldBe Seq("aylesbury")
      f.schema shouldBe Schema("location")
    }
    "support column projection expressions" in {
      val frame = Frame.fromValues(
        Schema("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      val f = frame.projectionExpression("location,name")
      f.head.values shouldBe Vector("aylesbury", "sam")
      f.schema shouldBe Schema("location", "name")
    }
    "support column projection re-ordering" in {
      val frame = Frame.fromValues(
        Schema("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      val f = frame.projection("location", "name")
      f.schema shouldBe Schema("location", "name")
      f.head.values shouldBe Vector("aylesbury", "sam")
    }
    "support union" in {
      frame.union(frame).size shouldBe 4
      frame.union(frame).toSet shouldBe Set(
        Row(frame.schema, "1", "2"),
        Row(frame.schema, "3", "4"),
        Row(frame.schema, "1", "2"),
        Row(frame.schema, "3", "4")
      )
    }
    //    "support collect" in {
    //      frame.collect {
    //        case row if row.head == "3" => row
    //      }.size shouldBe 1
    //    }
    "support ++" in {
      frame.++(frame).size shouldBe 4
    }

//    "support except" in {
    //      val frame1 = Frame(
    //        Schema("name", "location"),
    //        List("sam", "aylesbury"),
    //        List("jam", "aylesbury"),
    //        List("ham", "buckingham")
    //      )
    //      val frame2 = Frame(
    //        Schema("landmark", "location"),
    //        List("x", "aylesbury"),
    //        List("y", "aylesbury"),
    //        List("z", "buckingham")
    //      )
    //      val schema = Schema("name")
    //      frame1.except(frame2).schema shouldBe schema
    //      frame1.except(frame2).toSeq.toSet shouldBe Set(
    //        Row(schema, "sam"),
    //        Row(schema, "jam"),
    //        Row(schema, "ham")
    //      )
    //    }
    "support take while with row predicate" in {
      val frame = Frame.fromValues(
        Schema("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.takeWhile(row => row.get(1) == "aylesbury").toSet shouldBe Set(
        Row(frame.schema, "sam", "aylesbury"),
        Row(frame.schema, "jam", "aylesbury")
      )
    }
    "support take while with column predicate" in {
      val frame = Frame.fromValues(
        Schema("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.takeWhile("location", _ == "aylesbury").toSet shouldBe Set(
        Row(frame.schema, "sam", "aylesbury"),
        Row(frame.schema, "jam", "aylesbury")
      )
    }
    "support drop while" in {
      val frame = Frame.fromValues(
        Schema("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.dropWhile(_.get(1) == "aylesbury").toList shouldBe List(Row(frame.schema, "ham", "buckingham"))
    }
    "support drop while with column predicate" in {
      val frame = Frame.fromValues(
        Schema("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.dropWhile("location", _ == "aylesbury").toList shouldBe List(
        Row(frame.schema, "ham", "buckingham")
      )
    }
    "support explode" in {
      val f = Frame.fromValues(
        Schema("name", "location"),
        List("sam", "aylesbury"),
        List("jam", "aylesbury"),
        List("ham", "buckingham")
      )
      f.explode(row => Seq(row, row)).toSet shouldBe {
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
      val f = Frame.fromValues(
        Schema("name", "location"),
        List("sam", null),
        List("jam", "aylesbury"),
        List(null, "buckingham")
      )
      f.replaceNullValues("foo").toSet shouldBe {
        Set(
          Row(f.schema, "sam", "foo"),
          Row(f.schema, "jam", "aylesbury"),
          Row(f.schema, "foo", "buckingham")
        )
      }
    }
    "throw an error if the column is not present while filtering" in {
      val frame = Frame.fromValues(
        Schema("name", "location"),
        List("sam", "aylesbury"),
        List("ham", "buckingham")
      )
      intercept[RuntimeException] {
        frame.filter("bibble", row => true).toList
      }
    }
    "support replace" in {
      val frame = Frame.fromValues(
        Schema("name", "location"),
        List("sam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.replace("sam", "ham").toSet shouldBe {
        Set(
          Row(frame.schema, "ham", "aylesbury"),
          Row(frame.schema, "ham", "buckingham")
        )
      }
    }
    "support replace by column" in {
      val frame = Frame.fromValues(
        Schema("name", "location"),
        List("sam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.replace("name", "sam", "ham").toSet shouldBe {
        Set(
          Row(frame.schema, "ham", "aylesbury"),
          Row(frame.schema, "ham", "buckingham")
        )
      }
    }
    "support replace by column with function" in {
      val frame = Frame.fromValues(
        Schema("name", "location"),
        List("sam", "aylesbury"),
        List("ham", "buckingham")
      )
      val fn: Any => Any = any => any.toString.reverse
      frame.replace("name", fn).toSet shouldBe {
        Set(
          Row(frame.schema, "mas", "aylesbury"),
          Row(frame.schema, "mah", "buckingham")
        )
      }
    }
    "support dropNullRows" in {
      val frame = Frame.fromValues(
        Schema("name", "location"),
        List("sam", null),
        List("ham", "buckingham")
      )
      frame.dropNullRows.toList shouldBe {
        List(
          Row(frame.schema, "ham", "buckingham")
        )
      }
    }
    "support column update" in {
      val frame = Frame.fromValues(
        Schema("name", "location"),
        List("sam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.updateField(Field("name", FieldType.Int, true)).schema shouldBe Schema(Field("name", FieldType.Int, true), Field("location"))
    }
    "support column rename" in {
      val frame = Frame.fromValues(
        Schema("name", "location"),
        List("sam", "aylesbury"),
        List("ham", "buckingham")
      )
      frame.renameField("name", "blame").schema shouldBe Schema("blame", "location")
    }
    "convert from a Seq[T<:Product]" ignore {

      val p1 = Person("name1", 2, 1.2, true, 11, 3, 1)
      val p2 = Person("name2", 3, 11.2, true, 11111, 3121, 436541)
      val ps = Seq(p1, p2)

      val frame = Frame(ps)

      val rows = frame.toList()
      rows.size shouldBe 2
      rows shouldBe Seq(
        Row(frame.schema, Seq("name1", 2, 1.2, true, 11, 3, 1)),
        Row(frame.schema, Seq("name2", 3, 11.2, true, 11111, 3121, 436541))
      )
    }
  }
}
