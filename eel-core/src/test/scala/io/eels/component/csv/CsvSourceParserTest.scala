//package io.eels.component.csv
//
//import org.scalatest.{Matchers, WordSpec}
//
//class CsvSourceParserTest extends WordSpec with Matchers {
//
//  "CsvSourceParser" should {
//    "parse csv url" in {
//      val url = "csv:some/path"
//      CsvSourceParser(url).get shouldBe CsvSourceBuilder("some/path", Map.empty)
//    }
//    "parse url with trailing ?" in {
//      val url = "csv:some/path?"
//      CsvSourceParser(url).get shouldBe CsvSourceBuilder("some/path", Map.empty)
//    }
//    "parse url with options" in {
//      val url = "csv:some/path?a=b&c=d"
//      CsvSourceParser(url).get shouldBe CsvSourceBuilder("some/path", Map("a" -> List("b"), "c" -> List("d")))
//    }
//    "not parse url with missing path" in {
//      CsvSourceParser("csv:?a=b") shouldBe None
//      CsvSourceParser("csv:") shouldBe None
//    }
//    "not parse url with incorrect scheme" in {
//      CsvSourceParser("qweqe:some/path") shouldBe None
//    }
//  }
//}
