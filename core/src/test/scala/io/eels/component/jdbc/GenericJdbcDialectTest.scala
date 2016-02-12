package io.eels.component.jdbc

import io.eels.{SchemaType, Column}
import org.scalatest.{WordSpec, Matchers}

class GenericJdbcDialectTest extends WordSpec with Matchers {

  "GenericJdbcDialect" should {
    "convert int field to int type" in {
      GenericJdbcDialect.toJdbcType(Column("a", SchemaType.Int, false, 0, 0)) shouldBe "int"
    }
    "convert short field to SMALLINT type" in {
      GenericJdbcDialect.toJdbcType(Column("a", SchemaType.Short, false, 0, 0)) shouldBe "smallint"
    }
    "convert String field to VARCHAR type using precision" in {
      GenericJdbcDialect.toJdbcType(Column("a", SchemaType.String, false, 442, 0)) shouldBe "varchar(442)"
    }
  }
}
