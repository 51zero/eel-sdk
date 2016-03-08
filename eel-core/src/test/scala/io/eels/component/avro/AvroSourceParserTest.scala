package io.eels.component.avro

import org.scalatest.{Matchers, WordSpec}

class AvroSourceParserTest extends WordSpec with Matchers {

  "AvroSourceParser" should {
    "parse avro url" in {
      val url = "avro:some/path"
      AvroSourceParser(url).get shouldBe AvroSourceBuilder("some/path", Map.empty)
    }
    // fix in scalax
    "parse url with trailing ?" in {
      val url = "avro:some/path?"
      AvroSourceParser(url).get shouldBe AvroSourceBuilder("some/path", Map.empty)
    }
    "parse url with options" in {
      val url = "avro:some/path?a=b&c=d"
      AvroSourceParser(url).get shouldBe AvroSourceBuilder("some/path", Map("a" -> List("b"), "c" -> List("d")))
    }
    "not parse url with missing path" in {
      AvroSourceParser("avro:?a=b") shouldBe None
      AvroSourceParser("avro:") shouldBe None
    }
    "not parse url with incorrect scheme" in {
      AvroSourceParser("csv:some/path") shouldBe None
      AvroSourceParser("parquet:some/path") shouldBe None
    }
  }
}
