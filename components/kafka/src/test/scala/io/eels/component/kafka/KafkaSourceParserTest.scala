package io.eels.component.kafka

import io.eels.component.hive.HiveSourceParser
import org.scalatest.{Matchers, WordSpec}

class KafkaSourceParserTest extends WordSpec with Matchers {

  "KafkaSourceParser" should {
    "parse url" in {
      val url = "kafka:localhost:9000,localhost:9001/topic1,topic2"
      KafkaSourceParser(url).get shouldBe KafkaSourceBuilder("localhost:9000,localhost:9001", Set("topic1", "topic2"), Map.empty)
    }
    // fix in scalax
    "parse url with trailing ?" ignore {
      val url = "kafka:localhost:9000,localhost:9001/topic1,topic2?"
      KafkaSourceParser(url).get shouldBe KafkaSourceBuilder("localhost:9000,localhost:9001", Set("topic1", "topic2"), Map.empty)
    }
    "parse with options" in {
      val url = "kafka:localhost:9000/topic1?a=b"
      KafkaSourceParser(url).get shouldBe KafkaSourceBuilder("localhost:9000", Set("topic1"), Map("a" -> List("b")))
    }
    "not parse url with missing broker list" in {
      val url = "kafka:/topic1,topic2"
      HiveSourceParser(url) shouldBe None
    }
    "not parse url with missing topics" in {
      val url = "kafka:localhost:9000,localhost:9001/"
      HiveSourceParser(url) shouldBe None
    }
  }
}
