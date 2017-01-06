package io.eels.component.parquet

import io.eels.schema._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{DecimalMetadata, MessageType, OriginalType, PrimitiveType}
import org.scalatest.{FlatSpec, Matchers}

class ParquetSchemaFnsTest extends FlatSpec with Matchers {

  "ParquetSchemaFns.toParquetSchema" should "store timestamps as INT96" in {
    val schema = StructType(Field("a", TimestampMillisType))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT96, "a"))
  }

  it should "store bytes as BINARY" in {
    val schema = StructType(Field("a", BinaryType))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "a"))
  }

  it should "store decimals as FIXED_LEN_BYTE_ARRAY with OriginalType.DECIMAL and precision and scale set" in {
    val schema = StructType(Field("a", DecimalType(20, 10)))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 9, "a", OriginalType.DECIMAL, new DecimalMetadata(20, 10), new org.apache.parquet.schema.Type.ID(1)))
  }

  it should "store big int as FIXED_LEN_BYTE_ARRAY with OriginalType.DECIMAL and precision set and scale 0" in {
    val schema = StructType(Field("a", BigIntType))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 20, "a", OriginalType.DECIMAL, new DecimalMetadata(38, 0), new org.apache.parquet.schema.Type.ID(1)))
  }

  it should "store char as BINARY with UTF8" in {
    val schema = StructType(Field("a", CharType(255)))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "a", OriginalType.UTF8))
  }

  it should "store varchar as BINARY with UTF8" in {
    val schema = StructType(Field("a", VarcharType(255)))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "a", OriginalType.UTF8))
  }

  it should "store times as INT32 with original type tag TIME_MILLIS" in {
    val schema = StructType(Field("a", TimeMillisType))
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

  "ParquetSchemaFns.byteSizeForPrecision" should "calculate fixed length bytes required from precision" in {
    ParquetSchemaFns.byteSizeForPrecision(38) shouldBe 16
  }
}
