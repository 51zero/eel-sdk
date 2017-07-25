package io.eels

import io.eels.schema.StructType
import org.scalatest.{FunSuite, Matchers}

class RowTest extends FunSuite with Matchers {

  test("row.apply(int) should return correct field value by index") {
    val schema = StructType("a", "b", "c")
    val row = Row(schema, Vector(1, 2, 3))
    row(0) shouldBe 1
    row(1) shouldBe 2
    row(2) shouldBe 3
  }

  test("row.apply(String) should return correct field value by name") {
    val schema = StructType("a", "b", "c")
    val row = Row(schema, Vector(1, 2, 3))
    row("a") shouldBe 1
    row("b") shouldBe 2
    row("c") shouldBe 3
  }
}
