package io.eels.component.hive

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.kotlintest.specs.FunSpec

class HiveSchemasFnTest : FunSpec() {
  init {
    test("StructDDL should be valid") {
      val field = Field.createStruct("bibble", Field("a", FieldType.String), Field("b", FieldType.Double))
      val ddl = HiveSchemaFns.toStructDDL(field)
      ddl shouldBe "struct<a:string,b:double>"
    }
  }
}