package io.eels.component.jdbc.dialect

import org.scalatest.{FunSuite, Matchers}

class OracleJdbcDialectTest extends FunSuite with Matchers {

  test("oracle dialect should handle nulls") {
    new OracleJdbcDialect().sanitize(null: AnyRef) == null shouldBe true
  }
}
