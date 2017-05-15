package io.eels.component

import org.scalatest.{FunSuite, Matchers}

class ComponentUriTest extends FunSuite with Matchers {

  test("component parsing") {
    ComponentUri("csv:somepath") shouldBe ComponentUri("csv", "somepath", Map.empty)
    ComponentUri("csv:somepath?a=foo&b=moo") shouldBe ComponentUri("csv", "somepath", Map("a" -> "foo", "b" -> "moo"))
  }

  test("should throw error on malformed uri") {
    intercept[RuntimeException] {
      ComponentUri("csvsomepath")
    }.getMessage shouldBe "Invalid uri csvsomepath"
  }
}
