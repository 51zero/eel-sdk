//package io.eels
//
//import java.util.concurrent.atomic.AtomicInteger
//
//import io.eels.schema._
//import org.scalatest.concurrent.Eventually
//import org.scalatest.{Matchers, WordSpec}
//
//case class Wibble(name: String, location: String, postcode: String)
//
//class FrameTest extends WordSpec with Matchers with Eventually {
//
//  val schema = StructType("a", "b")
//  val frame = Frame.fromValues(schema, Vector("1", "2"), Vector("3", "4"))
//

//

//
//  "Frame.removeField" should {
//    "remove column" in {
//      val frame = Frame.fromValues(
//        StructType("name", "location", "postcode"),
//        List("sam", "aylesbury", "hp22"),
//        List("ham", "buckingham", "mk10")
//      )
//      val f = frame.removeField("location")
//      f.schema shouldBe StructType("name", "postcode")
//      f.toSet shouldBe Set(Row(f.schema, "sam", "hp22"), Row(f.schema, "ham", "mk10"))
//    }
//    "not remove column if case is different" in {
//      val frame = Frame.fromValues(
//        StructType("name", "location", "postcode"),
//        List("sam", "aylesbury", "hp22"),
//        List("ham", "buckingham", "mk10")
//      )
//      val f = frame.removeField("POSTcode")
//      f.schema shouldBe StructType("name", "location", "postcode")
//      f.toSet shouldBe Set(Row(f.schema, "sam", "aylesbury", "hp22"), Row(f.schema, "ham", "buckingham", "mk10"))
//    }
//    "remove column with ignore case" in {
//      val frame = Frame.fromValues(
//        StructType("name", "location", "postcode"),
//        List("sam", "aylesbury", "hp22"),
//        List("ham", "buckingham", "mk10")
//      )
//      val f = frame.removeField("locATION", false)
//      f.schema shouldBe StructType("name", "postcode")
//      f.toSet shouldBe Set(Row(f.schema, "sam", "hp22"), Row(f.schema, "ham", "mk10"))
//    }
//  }
//
////  "Frame.toSet[T]" should {
////    "marshall result into instances of T" in {
////      val frame = Frame(
////        Schema("name", "location", "postcode"),
////        List("sam", "aylesbury", "hp22"),
////        List("ham", "buckingham", "mk10")
////      )
////      frame.toSetAs[Wibble] shouldBe Set(Wibble("sam", "aylesbury", "hp22"), Wibble("ham", "buckingham", "mk10"))
////    }
////  }
////
////  "Frame.toSeq[T]" should {
////    "marshall result into instances of T" in {
////      val frame = Frame(
////        Schema("name", "location", "postcode"),
////        List("ham", "buckingham", "mk10")
////      )
////      frame.toSeqAs[Wibble] shouldBe Seq(Wibble("ham", "buckingham", "mk10"))
////    }
////  }
//
//  "Frame.filter" should {
//    "support row filtering by column name and fn" in {
//      frame.filter("b", _ == "2").size shouldBe 1
//    }
//
//  }
//
//
//  "Frame" should {
//    "be immutable and repeatable" in {
//      val f = frame.drop(1)
//      f.drop(1).size shouldBe 0
//      f.size shouldBe 1
//    }
//    "support forall" in {
//      frame.forall(_.size == 1) shouldBe false
//      frame.forall(_.size == 2) shouldBe true
//    }
//    "support exists" in {
//      frame.exists(_.size == 1) shouldBe false
//      frame.exists(_.size == 2) shouldBe true
//    }
//    "be thread safe when using drop" in {
//      val schema = StructType("k")
//      val rows = Iterator.tabulate(10000)(k => Row(schema, Vector("1"))).toList
//      val frame = Frame(schema, rows)
//      frame.drop(100).size shouldBe 9900
//    }


//    //    "support collect" in {
//    //      frame.collect {
//    //        case row if row.head == "3" => row
//    //      }.size shouldBe 1
//    //    }
//    "support ++" in {
//      frame.++(frame).size shouldBe 4
//    }
//
////    "support except" in {
//    //      val frame1 = Frame(
//    //        Schema("name", "location"),
//    //        List("sam", "aylesbury"),
//    //        List("jam", "aylesbury"),
//    //        List("ham", "buckingham")
//    //      )
//    //      val frame2 = Frame(
//    //        Schema("landmark", "location"),
//    //        List("x", "aylesbury"),
//    //        List("y", "aylesbury"),
//    //        List("z", "buckingham")
//    //      )
//    //      val schema = Schema("name")
//    //      frame1.except(frame2).schema shouldBe schema
//    //      frame1.except(frame2).toSeq.toSet shouldBe Set(
//    //        Row(schema, "sam"),
//    //        Row(schema, "jam"),
//    //        Row(schema, "ham")
//    //      )
//    //    }


//    "throw an error if the column is not present while filtering" in {
//      val frame = Frame.fromValues(
//        StructType("name", "location"),
//        List("sam", "aylesbury"),
//        List("ham", "buckingham")
//      )
//      intercept[RuntimeException] {
//        frame.filter("bibble", row => true).toList
//      }
//    }
//    "support replace" in {
//      val frame = Frame.fromValues(
//        StructType("name", "location"),
//        List("sam", "aylesbury"),
//        List("ham", "buckingham")
//      )
//      frame.replace("sam", "ham").toSet shouldBe {
//        Set(
//          Row(frame.schema, "ham", "aylesbury"),
//          Row(frame.schema, "ham", "buckingham")
//        )
//      }
//    }
//    "support replace by column" in {
//      val frame = Frame.fromValues(
//        StructType("name", "location"),
//        List("sam", "aylesbury"),
//        List("ham", "buckingham")
//      )
//      frame.replace("name", "sam", "ham").toSet shouldBe {
//        Set(
//          Row(frame.schema, "ham", "aylesbury"),
//          Row(frame.schema, "ham", "buckingham")
//        )
//      }
//    }
//    "support replace by column with function" in {
//      val frame = Frame.fromValues(
//        StructType("name", "location"),
//        List("sam", "aylesbury"),
//        List("ham", "buckingham")
//      )
//      val fn: Any => Any = any => any.toString.reverse
//      frame.replace("name", fn).toSet shouldBe {
//        Set(
//          Row(frame.schema, "mas", "aylesbury"),
//          Row(frame.schema, "mah", "buckingham")
//        )
//      }
//    }
//    "support dropNullRows" in {
//      val frame = Frame.fromValues(
//        StructType("name", "location"),
//        List("sam", null),
//        List("ham", "buckingham")
//      )
//      frame.dropNullRows.toList shouldBe {
//        List(
//          Row(frame.schema, "ham", "buckingham")
//        )
//      }
//    }
//    "support column update" in {
//      val frame = Frame.fromValues(
//        StructType("name", "location"),
//        List("sam", "aylesbury"),
//        List("ham", "buckingham")
//      )
//      frame.updateField(Field("name", BooleanType, true)).schema shouldBe
//        StructType(Field("name", BooleanType, true), Field("location", StringType))
//    }
//    "convert from a Seq[T<:Product]" ignore {
//
//      val p1 = Person("name1", 2, 1.2, true, 11, 3, 1)
//      val p2 = Person("name2", 3, 11.2, true, 11111, 3121, 436541)
//      val ps = Seq(p1, p2)
//
//      val frame = Frame(ps)
//
//      val rows = frame.collect()
//      rows.size shouldBe 2
//      rows shouldBe Seq(
//        Row(frame.schema, Seq("name1", 2, 1.2, true, 11, 3, 1)),
//        Row(frame.schema, Seq("name2", 3, 11.2, true, 11111, 3121, 436541))
//      )
//    }
//  }
//}
