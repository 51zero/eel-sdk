package io.eels.component.hive

import io.eels.schema._
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.scalatest.{Matchers, WordSpec}

class HiveSchemaFnsTest extends WordSpec with Matchers {

  "HiveSchemaFns" should {
    "StructDDL should be valid" in {
      val fields = Vector(Field("a", StringType), Field("b", DoubleType))
      val ddl = HiveSchemaFns.toStructDDL(fields)
      ddl shouldBe "struct<a:string,b:double>"
    }

    "toHiveField(field) should return correct hive type" in {
      HiveSchemaFns.toHiveField(Field("a", dataType = BooleanType)) shouldBe new FieldSchema("a", "boolean", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = BinaryType)) shouldBe new FieldSchema("a", "string", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = DecimalType(Precision(2), Scale(1)))) shouldBe new FieldSchema("a", "decimal(2,1)", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = DateType)) shouldBe new FieldSchema("a", "date", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = DoubleType)) shouldBe new FieldSchema("a", "double", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = FloatType)) shouldBe new FieldSchema("a", "float", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = IntType(true))) shouldBe new FieldSchema("a", "int", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = LongType(true))) shouldBe new FieldSchema("a", "bigint", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = TimestampType)) shouldBe new FieldSchema("a", "timestamp", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = StringType)) shouldBe new FieldSchema("a", "string", null)
    }

    "fromHiveField should support decimals" in {
      HiveSchemaFns.fromHiveField(new FieldSchema("a", "decimal(10,5)", null), true) shouldBe Field("a", dataType = DecimalType(Precision(10), Scale(5)))
    }

    "fromHiveField should support varchar" in {
      HiveSchemaFns.fromHiveField(new FieldSchema("a", "varchar(200)", null), true) shouldBe Field("a", dataType = VarcharType(200))
    }

    "fromHiveField should support structs" in {
      val fs = new FieldSchema("structy_mcstructface", "struct<a:string,b:double>", "commy")
      HiveSchemaFns.fromHiveField(fs, true) shouldBe
        Field(
          name = "structy_mcstructface",
          dataType = StructType(
            Field(name = "a", dataType = StringType, nullable = true),
            Field(name = "b", dataType = DoubleType, nullable = true)
          ),
          nullable = true,
          comment = Option("commy")
        )
    }
  }
}
