package com.sksamuel.eel.elasticsearch.sink

import com.sksamuel.eel.Row
import org.scalatest.{WordSpec, Matchers}

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
