package io.eels.component.hive

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Precision
import io.eels.schema.Scale
import io.kotlintest.specs.FunSpec
import org.apache.hadoop.hive.metastore.api.FieldSchema

class HiveSchemasFnTest : FunSpec() {
  init {

    test("StructDDL should be valid") {
      val field = Field.createStruct("bibble", Field("a", FieldType.String), Field("b", FieldType.Double))
      val ddl = HiveSchemaFns.toStructDDL(field)
      ddl shouldBe "struct<a:string,b:double>"
    }

    test("fromHiveField should support structs") {
      val fs = FieldSchema("structy_mcstructface", "struct<a:string,b:double>", "commy")
      HiveSchemaFns.fromHiveField(fs, true) shouldBe
          Field(
              name = "structy_mcstructface",
              type = FieldType.Struct,
              nullable = true,
              precision = Precision(value = 0),
              scale = Scale(value = 0),
              signed = false,
              arrayType = null,
              fields = listOf(
                  Field(name = "a", type = FieldType.String, nullable = true, precision = Precision(value = 0), scale = Scale(value = 0)),
                  Field(name = "b", type = FieldType.Double, nullable = true, precision = Precision(value = 0), scale = Scale(value = 0))
              ),
              comment = "commy"
          )
    }
  }
}