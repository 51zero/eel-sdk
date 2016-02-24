package io.eels.component.hive

import org.scalatest.{Matchers, WordSpec}

class HiveSourceParserTest extends WordSpec with Matchers {

  "HiveSourceParser" should {
    "parse hive url" in {
      val url = "hive:sam:ham"
      HiveSourceParser(url).get shouldBe HiveSourceBuilder("sam", "ham", Map.empty)
    }
    "parse hive url with trailing ?" in {
      val url = "hive:sam:ham?"
      HiveSourceParser(url).get shouldBe HiveSourceBuilder("sam", "ham", Map.empty)
    }
    "parse hive with options" in {
      val url = "hive:sam:ham?a=b&c=d"
      HiveSourceParser(url).get shouldBe HiveSourceBuilder("sam", "ham", Map("a" -> List("b"), "c" -> List("d")))
    }
    "parse not parse url with missing db" in {
      val url = "hive::tab"
      HiveSourceParser(url) shouldBe None
    }
    "parse not parse url with missing table" in {
      val url = "hive:sam:"
      HiveSourceParser(url) shouldBe None
    }
  }
}
