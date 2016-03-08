package io.eels.component.csv

import org.scalatest.{Matchers, WordSpec}

class CsvSinkParserTest extends WordSpec with Matchers {

  "CsvSinkParser" should {
    "parse url" in {
      val url = "csv:some/path"
      CsvSinkParser(url).get shouldBe CsvSinkBuilder("some/path", Map.empty)
    }
    "parse url with trailing ?" in {
      val url = "csv:some/path?"
      CsvSinkParser(url).get shouldBe CsvSinkBuilder("some/path", Map.empty)
    }
    "parse url with options" in {
      val url = "csv:some/path?a=b&c=d"
      CsvSinkParser(url).get shouldBe CsvSinkBuilder("some/path", Map("a" -> List("b"), "c" -> List("d")))
    }
    "not parse url with missing path" in {
      CsvSinkParser("csv:?a=b") shouldBe None
      CsvSinkParser("csv:") shouldBe None
    }
    "not parse url with incorrect scheme" in {
      CsvSinkParser("qweqe:some/path") shouldBe None
    }
  }
}
