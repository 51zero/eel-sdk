package io.eels.component.parquet

import io.eels.schema._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, OriginalType, PrimitiveType}
import org.scalatest.{FlatSpec, Matchers}

class ParquetSchemaFnsTest extends FlatSpec with Matchers {

  "ParquetSchemaFns.toParquetSchema" should "store timestamps as int64 with original type tag" in {
    val schema = StructType(Field("a", TimestampType))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, "a", OriginalType.TIMESTAMP_MILLIS))
  }

  it should "store times as INT32 with original type tag TIME_MILLIS" in {
    val schema = StructType(Field("a", TimeType))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "a", OriginalType.TIME_MILLIS))
  }

  it should "store doubles as DOUBLE" in {
    val schema = StructType(Field("a", DoubleType))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "a"))
  }

  it should "store booleans as BOOLEAN" in {
    val schema = StructType(Field("a", BooleanType))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BOOLEAN, "a"))
  }

  it should "store floats as FLOAT" in {
    val schema = StructType(Field("a", FloatType))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.FLOAT, "a"))
  }

  it should "store signed shorts as INT32 with original type INT_16" in {
    val schema = StructType(Field("a", ShortType(true)))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "a", OriginalType.INT_16))
  }

  it should "store unsigned shorts as INT32 with unsigned original type UINT_16" in {
    val schema = StructType(Field("a", ShortType(false)))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "a", OriginalType.UINT_16))
  }

  it should "store signed ints as INT32" in {
    val schema = StructType(Field("a", IntType(true)))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "a"))
  }

  it should "store unsigned ints as INT32 with unsigned original type UINT_32" in {
    val schema = StructType(Field("a", IntType(false)))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "a", OriginalType.UINT_32))
  }

  it should "store signed longs as INT64" in {
    val schema = StructType(Field("a", LongType(true)))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, "a"))
  }

  it should "store unsigned longs as INT64 with unsigned original type" in {
    val schema = StructType(Field("a", LongType(false)))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, "a", OriginalType.UINT_64))
  }

  it should "store dates as int32 with original type tag DATE" in {
    val schema = StructType(Field("a", DateType))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "a", OriginalType.DATE))
  }

  it should "store Strings as Binary with original type tag UTF8" in {
    val schema = StructType(Field("a", StringType))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "a", OriginalType.UTF8))
  }
}
