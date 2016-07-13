package io.eels.component.jdbc

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Precision
import io.eels.schema.Scale
import io.kotlintest.specs.WordSpec

class GenericJdbcDialectTest : WordSpec() {
  init {
    "GenericJdbcDialect" should {
      "convert int field to int type"  {
        GenericJdbcDialect().toJdbcType(Field("a", FieldType.Int, false)) shouldBe "int"
      }
      "convert Boolean field to int type"  {
        GenericJdbcDialect().toJdbcType(Field("b", FieldType.Boolean, false)) shouldBe "BOOLEAN"
      }
      "convert short field to SMALLINT type"  {
        GenericJdbcDialect().toJdbcType(Field("a", FieldType.Short, false)) shouldBe "smallint"
      }
      "convert String field to VARCHAR type using precision"  {
        GenericJdbcDialect().toJdbcType(Field("a", FieldType.String, false, Precision(442), Scale(0))) shouldBe "varchar(442)"
      }
    }
  }
}
