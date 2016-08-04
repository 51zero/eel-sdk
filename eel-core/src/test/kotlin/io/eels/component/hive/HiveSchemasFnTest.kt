package io.eels.component.hive

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Precision
import io.eels.schema.Scale
import io.kotlintest.specs.StringSpec
import org.apache.hadoop.hive.metastore.api.FieldSchema

class HiveSchemasFnTest : StringSpec() {
  init {

    "StructDDL should be valid" {
      val field = Field.createStruct("bibble", Field("a", FieldType.String), Field("b", FieldType.Double))
      val ddl = HiveSchemaFns.toStructDDL(field)
      ddl shouldBe "struct<a:string,b:double>"
    }

    "toHiveField(field) should return correct hive type" {
      HiveSchemaFns.toHiveField(Field("a", type = FieldType.Boolean)) shouldBe FieldSchema("a", "boolean", null)
      HiveSchemaFns.toHiveField(Field("a", type = FieldType.Binary)) shouldBe FieldSchema("a", "string", null)
      HiveSchemaFns.toHiveField(Field("a", type = FieldType.Decimal, scale = Scale(2), precision = Precision(3))) shouldBe FieldSchema("a", "decimal(2,3)", null)
      HiveSchemaFns.toHiveField(Field("a", type = FieldType.Date)) shouldBe FieldSchema("a", "date", null)
      HiveSchemaFns.toHiveField(Field("a", type = FieldType.Double)) shouldBe FieldSchema("a", "double", null)
      HiveSchemaFns.toHiveField(Field("a", type = FieldType.Float)) shouldBe FieldSchema("a", "float", null)
      HiveSchemaFns.toHiveField(Field("a", type = FieldType.Int)) shouldBe FieldSchema("a", "int", null)
      HiveSchemaFns.toHiveField(Field("a", type = FieldType.Long)) shouldBe FieldSchema("a", "bigint", null)
      HiveSchemaFns.toHiveField(Field("a", type = FieldType.Timestamp)) shouldBe FieldSchema("a", "timestamp", null)
      HiveSchemaFns.toHiveField(Field("a", type = FieldType.String, precision = Precision(5))) shouldBe FieldSchema("a", "string", null)
    }

    "fromHiveField should support decimals" {
      HiveSchemaFns.fromHiveField(FieldSchema("a", "decimal(10,5)", null), true) shouldBe Field("a", type = FieldType.Decimal, scale = Scale(10), precision = Precision(5))
    }

    "fromHiveField should support varchar" {
      HiveSchemaFns.fromHiveField(FieldSchema("a", "varchar(200)", null), true) shouldBe Field("a", type = FieldType.String, precision = Precision(200))
    }

    "fromHiveField should support structs" {
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