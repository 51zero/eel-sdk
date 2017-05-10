package io.eels.source

import org.scalatest.{FunSuite, Matchers}

class SourceTest extends FunSuite with Matchers {

  test("source parsing") {
    Source("csv:somepath") shouldBe Source("csv:somepath", Map.empty)
    Source("csv:somepath?a=foo&b=moo") shouldBe Source("csv:somepath", Map("a" -> "foo", "b" -> "moo"))
  }
}
