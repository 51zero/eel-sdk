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
      HiveSchemaFns.toHiveField(Field("a", dataType = BinaryType)) shouldBe new FieldSchema("a", "binary", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = ByteType(true))) shouldBe new FieldSchema("a", "tinyint", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = DecimalType(Precision(2), Scale(1)))) shouldBe new FieldSchema("a", "decimal(2,1)", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = DateType)) shouldBe new FieldSchema("a", "date", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = DoubleType)) shouldBe new FieldSchema("a", "double", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = FloatType)) shouldBe new FieldSchema("a", "float", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = IntType(true))) shouldBe new FieldSchema("a", "int", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = LongType(true))) shouldBe new FieldSchema("a", "bigint", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = ShortType(true))) shouldBe new FieldSchema("a", "smallint", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = StringType)) shouldBe new FieldSchema("a", "string", null)
      HiveSchemaFns.toHiveField(Field("a", dataType = TimestampMillisType)) shouldBe new FieldSchema("a", "timestamp", null)
    }

    "fromHiveField should support decimals" in {
      HiveSchemaFns.fromHiveType("decimal(10,5)") shouldBe DecimalType(Precision(10), Scale(5))
    }

    "fromHiveField should support varchar" in {
      HiveSchemaFns.fromHiveType("varchar(200)") shouldBe VarcharType(200)
    }

    "fromHiveField should support char" in {
      HiveSchemaFns.fromHiveType("char(200)") shouldBe CharType(200)
    }

    "fromHiveField should support primitive arrays" in {
      HiveSchemaFns.fromHiveType("array<string>") shouldBe ArrayType(StringType)
    }

    "fromHiveField should support complex arrays" in {
      HiveSchemaFns.fromHiveType("array<struct<a:string,b:double>>") shouldBe ArrayType(StructType(
        Field(name = "a", dataType = StringType, nullable = true),
        Field(name = "b", dataType = DoubleType, nullable = true)
      ))
    }

    "fromHiveField should support structs" in {
      HiveSchemaFns.fromHiveType("struct<a:string,b:double>") shouldBe
        StructType(
          Field(name = "a", dataType = StringType, nullable = true),
          Field(name = "b", dataType = DoubleType, nullable = true)
        )
    }
  }
}
