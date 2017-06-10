//package io.eels.component.parquet
//
//import org.apache.hadoop.fs.Path
//import org.scalatest.{Matchers, WordSpec}
//
//class ParquetSinkParserTest extends WordSpec with Matchers {
//
//  "ParquetSinkParser" should {
//    "parse url" in {
//      val url = "parquet:some/path"
//      ParquetSinkParser(url).get shouldBe ParquetSinkBuilder(new Path("some/path"), Map.empty)
//    }
//    // fix in scalax
//    "parse url with trailing ?" in {
//      val url = "parquet:some/path?"
//      ParquetSinkParser(url).get shouldBe ParquetSinkBuilder(new Path("some/path"), Map.empty)
//    }
//    "parse url with options" in {
//      val url = "parquet:some/path?a=b&c=d"
//      ParquetSinkParser(url).get shouldBe ParquetSinkBuilder(new Path("some/path"), Map("a" -> List("b"), "c" -> List("d")))
//    }
//    "not parse url with missing path" in {
//      ParquetSinkParser("parquet:?a=b") shouldBe None
//      ParquetSinkParser("parquet:") shouldBe None
//    }
//    "not parse url with incorrect scheme" in {
//      ParquetSinkParser("csv:some/path") shouldBe None
//    }
//  }
//}
