package io.eels.component.hive

import io.eels.schema.{Field, FieldType, Precision, Scale}
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.scalatest.{Matchers, WordSpec}

class HiveSchemaFnsTest extends WordSpec with Matchers {

  "HiveSchemaFns" should {
    "StructDDL should be valid" in {
      val field = Field.createStruct("bibble", Field("a", FieldType.String), Field("b", FieldType.Double))
      val ddl = HiveSchemaFns.toStructDDL(field)
      ddl shouldBe "struct<a:string,b:double>"
    }

    "toHiveField(field) should return correct hive type" in {
      HiveSchemaFns.toHiveField(Field("a", `type` = FieldType.Boolean)) shouldBe new FieldSchema("a", "boolean", null)
      HiveSchemaFns.toHiveField(Field("a", `type` = FieldType.Binary)) shouldBe new FieldSchema("a", "string", null)
      HiveSchemaFns.toHiveField(Field("a", `type` = FieldType.Decimal, scale = Scale(2), precision = Precision(3))) shouldBe new FieldSchema("a", "decimal(2,3)", null)
      HiveSchemaFns.toHiveField(Field("a", `type` = FieldType.Date)) shouldBe new FieldSchema("a", "date", null)
      HiveSchemaFns.toHiveField(Field("a", `type` = FieldType.Double)) shouldBe new FieldSchema("a", "double", null)
      HiveSchemaFns.toHiveField(Field("a", `type` = FieldType.Float)) shouldBe new FieldSchema("a", "float", null)
      HiveSchemaFns.toHiveField(Field("a", `type` = FieldType.Int)) shouldBe new FieldSchema("a", "int", null)
      HiveSchemaFns.toHiveField(Field("a", `type` = FieldType.Long)) shouldBe new FieldSchema("a", "bigint", null)
      HiveSchemaFns.toHiveField(Field("a", `type` = FieldType.Timestamp)) shouldBe new FieldSchema("a", "timestamp", null)
      HiveSchemaFns.toHiveField(Field("a", `type` = FieldType.String, precision = Precision(5))) shouldBe new FieldSchema("a", "string", null)
    }

    "fromHiveField should support decimals" in {
      HiveSchemaFns.fromHiveField(new FieldSchema("a", "decimal(10,5)", null), true) shouldBe Field("a", `type` = FieldType.Decimal, scale = Scale(10), precision = Precision(5))
    }

    "fromHiveField should support varchar" in {
      HiveSchemaFns.fromHiveField(new FieldSchema("a", "varchar(200)", null), true) shouldBe Field("a", `type` = FieldType.String, precision = Precision(200))
    }

    "fromHiveField should support structs" in {
      val fs = new FieldSchema("structy_mcstructface", "struct<a:string,b:double>", "commy")
      HiveSchemaFns.fromHiveField(fs, true) shouldBe
        Field(
          name = "structy_mcstructface",
          `type` = FieldType.Struct,
          nullable = true,
          precision = Precision(value = 0),
          scale = Scale(value = 0),
          signed = false,
          arrayType = null,
          fields = List(
            Field(name = "a", `type` = FieldType.String, nullable = true, precision = Precision(value = 0), scale = Scale(value = 0)),
            Field(name = "b", `type` = FieldType.Double, nullable = true, precision = Precision(value = 0), scale = Scale(value = 0))
          ),
          comment = Some("commy")
        )
    }
  }
}
