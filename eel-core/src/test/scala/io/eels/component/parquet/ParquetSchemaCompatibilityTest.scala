package io.eels.component.parquet

import io.eels.schema._
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._
import org.scalatest.{FunSuite, Matchers}

// tests that the eel <-> parquet schemas are compatible
class ParquetSchemaCompatibilityTest extends FunSuite with Matchers {

  test("parquet schema should be compatible with eel struct types") {

    val messageType = new MessageType(
      "eel_schema",
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "requiredBinary"),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BOOLEAN, "requiredBoolean"),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "requiredDouble"),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FLOAT, "requiredFloat"),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "requiredInt"),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "requiredLong"),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "requiredString", OriginalType.UTF8),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "reqEnum", OriginalType.ENUM),
      Types.required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).precision(14).scale(6).id(1).length(6).as(OriginalType.DECIMAL).named("requiredDecimal"),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "timeMillis", OriginalType.TIME_MILLIS),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "timeMicros", OriginalType.TIME_MICROS),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT96, "timestampMillis"),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "timestampMicros", OriginalType.TIMESTAMP_MICROS)
    )

    val struct = StructType(Vector(
      Field("requiredBinary", BinaryType, false, false),
      Field("requiredBoolean", BooleanType, false, false),
      Field("requiredDouble", DoubleType, false, false),
      Field("requiredFloat", FloatType, false, false),
      Field("requiredInt", IntType(true), false, false),
      Field("requiredLong", LongType(true), false, false),
      Field("requiredString", StringType, false, false),
      Field("reqEnum", EnumType("reqEnum", Nil), false, false),
      Field("requiredDecimal", DecimalType(14, 6), false, false),
      Field("timeMillis", TimeMillisType, false, false),
      Field("timeMicros", TimeMicrosType, false, false),
      Field("timestampMillis", TimestampMillisType, false, false),
      Field("timestampMicros", TimestampMicrosType, false, false)
    ))

    ParquetSchemaFns.fromParquetMessageType(messageType) shouldBe struct
    ParquetSchemaFns.toParquetMessageType(struct) shouldBe messageType
  }

  test("parquet schema fns should convert char and varchar to strings") {

    val messageType = new MessageType(
      "eel_schema",
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "varchar", OriginalType.UTF8),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "char", OriginalType.UTF8)
    )

    val outputStruct = StructType(Vector(
      Field("varchar", StringType, false),
      Field("char", StringType, false)
    ))

    val inputStruct = StructType(Vector(
      Field("varchar", VarcharType(244), false),
      Field("char", CharType(15), false)
    ))

    ParquetSchemaFns.fromParquetMessageType(messageType) shouldBe outputStruct
    ParquetSchemaFns.toParquetMessageType(inputStruct) shouldBe messageType
  }
}
