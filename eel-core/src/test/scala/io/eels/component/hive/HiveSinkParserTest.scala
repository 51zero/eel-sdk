//package io.eels.component.hive
//
//import org.scalatest.{Matchers, WordSpec}
//
//class HiveSinkParserTest extends WordSpec with Matchers {
//
//  "HiveSinkParser" should {
//    "parse hive url" in {
//      val url = "hive:sam:ham"
//      HiveSinkParser(url).get shouldBe HiveSinkBuilder("sam", "ham", Map.empty)
//    }
//    // fix in scalax
//    "parse hive url with trailing ?" ignore {
//      val url = "hive:sam:ham?"
//      HiveSinkParser(url).get shouldBe HiveSinkBuilder("sam", "ham", Map.empty)
//    }
//    "parse hive with options" in {
//      val url = "hive:sam:ham?a=b&c=d"
//      HiveSinkParser(url).get shouldBe HiveSinkBuilder("sam", "ham", Map("a" -> List("b"), "c" -> List("d")))
//    }
//    "parse not parse url with missing db" in {
//      val url = "hive::tab"
//      HiveSinkParser(url) shouldBe None
//    }
//    "parse not parse url with missing table" in {
//      val url = "hive:sam:"
//      HiveSinkParser(url) shouldBe None
//    }
//  }
//}
