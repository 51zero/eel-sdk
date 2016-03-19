package io.eels.component.hive

import io.eels.SchemaType
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.scalatest.{Matchers, WordSpec}

class HiveSchemaFnsTest extends WordSpec with Matchers {

  "HiveSchemaFns" should {
    "convert binary to binary" in {
      HiveSchemaFns.fromHiveField(new FieldSchema("name", "binary", null), true).`type` shouldBe SchemaType.Binary
    }
    "convert boolean to boolean" in {
      HiveSchemaFns.fromHiveField(new FieldSchema("name", "boolean", null), true).`type` shouldBe SchemaType.Boolean
    }
    "convert smallint to short" in {
      HiveSchemaFns.fromHiveField(new FieldSchema("name", "smallint", null), true).`type` shouldBe SchemaType.Short
    }
    "convert int to int" in {
      HiveSchemaFns.fromHiveField(new FieldSchema("name", "int", null), true).`type` shouldBe SchemaType.Int
    }
    "convert bigint to bigint" in {
      HiveSchemaFns.fromHiveField(new FieldSchema("name", "bigint", null), true).`type` shouldBe SchemaType.BigInt
    }
    "convert varcar to short" in {
      val column = HiveSchemaFns.fromHiveField(new FieldSchema("name", "varchar(12)", null), true)
      column.`type` shouldBe SchemaType.String
      column.precision shouldBe 12
    }
    "parse hive decimal" in {
      val column = HiveSchemaFns.fromHiveField(new FieldSchema("name", "decimal(12,4)", null), true)
      column.`type` shouldBe SchemaType.Decimal
      column.scale shouldBe 4
      column.precision shouldBe 12
    }
    "convert float to float" in {
      HiveSchemaFns.fromHiveField(new FieldSchema("name", "float", null), true).`type` shouldBe SchemaType.Float
    }
    "convert double to double" in {
      HiveSchemaFns.fromHiveField(new FieldSchema("name", "double", null), true).`type` shouldBe SchemaType.Double
    }
    "convert date to date" in {
      HiveSchemaFns.fromHiveField(new FieldSchema("name", "date", null), true).`type` shouldBe SchemaType.Date
    }
    "convert timestamp to timestamp" in {
      HiveSchemaFns.fromHiveField(new FieldSchema("name", "timestamp", null), true).`type` shouldBe SchemaType.Timestamp
    }
  }
}
