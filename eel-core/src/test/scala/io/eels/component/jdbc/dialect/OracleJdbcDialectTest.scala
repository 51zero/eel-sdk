package io.eels.component.jdbc.dialect

import org.scalatest.{FunSuite, Matchers}

class OracleJdbcDialectTest extends FunSuite with Matchers {

  test("oracle dialect should handle nulls") {
    new OracleJdbcDialect().sanitize(null) == null shouldBe true
  }

  test("oracle dialect should handle strings as is") {
    new OracleJdbcDialect().sanitize("test") shouldBe "test"
  }
}
