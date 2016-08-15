package io.eels.component.jdbc

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Precision
import io.eels.schema.Scale
import org.scalatest.{Matchers, WordSpec}

class GenericJdbcDialectTest extends WordSpec with Matchers {
  "GenericJdbcDialect" should {
    "convert int field to int type" in {
      new GenericJdbcDialect().toJdbcType(Field("a", FieldType.Int, false)) shouldBe "int"
    }
    "convert Boolean field to int type" in {
      new GenericJdbcDialect().toJdbcType(Field("b", FieldType.Boolean, false)) shouldBe "BOOLEAN"
    }
    "convert short field to SMALLINT type" in {
      new GenericJdbcDialect().toJdbcType(Field("a", FieldType.Short, false)) shouldBe "smallint"
    }
    "convert String field to VARCHAR type using precision" in {
      new GenericJdbcDialect().toJdbcType(Field("a", FieldType.String, false, Precision(442), Scale(0))) shouldBe "varchar(442)"
    }
  }
}
