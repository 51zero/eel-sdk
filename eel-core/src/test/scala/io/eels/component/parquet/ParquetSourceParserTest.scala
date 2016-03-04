package io.eels.component.parquet

import org.scalatest.{Matchers, WordSpec}

class ParquetSourceParserTest extends WordSpec with Matchers {

  "ParquetSourceParser" should {
    "parse csv url" in {
      val url = "parquet:some/path"
      ParquetSourceParser(url).get shouldBe ParquetSourceBuilder("some/path", Map.empty)
    }
    // fix in scalax
    "parse url with trailing ?" in {
      val url = "parquet:some/path?"
      ParquetSourceParser(url).get shouldBe ParquetSourceBuilder("some/path", Map.empty)
    }
    "parse url with options" in {
      val url = "parquet:some/path?a=b&c=d"
      ParquetSourceParser(url).get shouldBe ParquetSourceBuilder("some/path", Map("a" -> List("b"), "c" -> List("d")))
    }
    "not parse url with missing path" in {
      ParquetSourceParser("parquet:?a=b") shouldBe None
      ParquetSourceParser("parquet:") shouldBe None
    }
    "not parse url with incorrect scheme" in {
      ParquetSourceParser("csv:some/path") shouldBe None
    }
  }
}
