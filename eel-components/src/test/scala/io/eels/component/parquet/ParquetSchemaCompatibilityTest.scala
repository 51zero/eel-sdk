package io.eels.component.parquet

import io.eels.schema._
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._
import org.scalatest.{FunSuite, Matchers}

// tests that the eel <-> parquet schemas are compatible
class ParquetSchemaCompatibilityTest extends FunSuite with Matchers {

  test("parquet schema should be compatible with eel struct types") {

    val messageType = new MessageType(
      "row",
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "requiredBinary"),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BOOLEAN, "requiredBoolean"),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "requiredDouble"),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FLOAT, "requiredFloat"),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "requiredInt"),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "requiredLong"),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "requiredString", OriginalType.UTF8),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 6, "requiredDecimal", OriginalType.DECIMAL, new DecimalMetadata(14, 6), new Type.ID(1)),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "timeMillis", OriginalType.TIME_MILLIS),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "timeMicros", OriginalType.TIME_MICROS),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT96, "timestampMillis"),
      new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "timestampMicros", OriginalType.TIMESTAMP_MICROS)
    )

    val structType = StructType(Vector(
      Field("requiredBinary", BinaryType, false, false, None, Map()),
      Field("requiredBoolean", BooleanType, false, false, None, Map()),
      Field("requiredDouble", DoubleType, false, false, None, Map()),
      Field("requiredFloat", FloatType, false, false, None, Map()),
      Field("requiredInt", IntType(true), false, false, None, Map()),
      Field("requiredLong", LongType(true), false, false, None, Map()),
      Field("requiredString", StringType, false, false, None, Map()),
      Field("requiredDecimal", DecimalType(14, 6), false, false, None, Map()),
      Field("timeMillis", TimeMillisType, false, false, None, Map()),
      Field("timeMicros", TimeMicrosType, false, false, None, Map()),
      Field("timestampMillis", TimestampMillisType, false, false, None, Map()),
      Field("timestampMicros", TimestampMicrosType, false, false, None, Map())
    ))

    ParquetSchemaFns.fromParquetGroupType(messageType) shouldBe structType
    ParquetSchemaFns.toParquetSchema(structType) shouldBe messageType
  }
}
