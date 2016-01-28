package io.eels.component.elasticsearch

import io.eels.Row
import org.scalatest.{Matchers, WordSpec}

class RowIndexableTest extends WordSpec with Matchers {

  "RowIndexable" should {
    "generate json for row" in {
      val row = Row(Map("a" -> "sam", "b" -> "ham"))
      val json = IndexableImplicits.RowIndexable.json(row)
      json shouldBe """{"a":"sam","b":"ham"}"""
    }
    "support escaping" in {
      val row = Row(Map("a" -> "sam", "b" -> """'"'"'"""))
      val json = IndexableImplicits.RowIndexable.json(row)
      json shouldBe """{"a":"sam","b":"'\"'\"'"}"""
    }
  }

}
