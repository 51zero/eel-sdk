package io.eels.component.jdbc

import io.eels.schema._
import org.scalatest.{Matchers, WordSpec}

class GenericJdbcDialectTest extends WordSpec with Matchers {
  "GenericJdbcDialect" should {
    "convert int field to int type" in {
      new GenericJdbcDialect().toJdbcType(Field("a", IntType(true), false)) shouldBe "int"
    }
    "convert Boolean field to int type" in {
      new GenericJdbcDialect().toJdbcType(Field("b", BooleanType, false)) shouldBe "BOOLEAN"
    }
    "convert short field to SMALLINT type" in {
      new GenericJdbcDialect().toJdbcType(Field("a", ShortType.Signed, false)) shouldBe "smallint"
    }
    "convert String field to text" in {
      new GenericJdbcDialect().toJdbcType(Field("a", StringType, false)) shouldBe "text"
    }
    "convert varchar field to VARCHAR using size" in {
      new GenericJdbcDialect().toJdbcType(Field("a", VarcharType(242), false)) shouldBe "varchar(242)"
    }
  }
}
