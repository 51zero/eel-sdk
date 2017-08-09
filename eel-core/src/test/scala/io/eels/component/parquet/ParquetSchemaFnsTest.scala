package io.eels.component.parquet

import io.eels.schema._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._
import org.scalatest.{FlatSpec, Matchers}

class ParquetSchemaFnsTest extends FlatSpec with Matchers {

  "toParquetMessageType" should "store timestamps as INT96" in {
    val schema = StructType(Field("a", TimestampMillisType))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType("eel_schema", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT96, "a"))
  }

  it should "store bytes as BINARY" in {
    val schema = StructType(Field("a", BinaryType))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType("eel_schema", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "a"))
  }

  it should "store decimals as FIXED_LEN_BYTE_ARRAY with OriginalType.DECIMAL and precision and scale set" in {
    val schema = StructType(Field("a", DecimalType(20, 10)))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType(
        "eel_schema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Repetition.OPTIONAL).precision(20).scale(10).id(1).length(9).as(OriginalType.DECIMAL).named("a")
    )
  }

  it should "store big int as FIXED_LEN_BYTE_ARRAY with OriginalType.DECIMAL and precision set and scale 0" in {
    val schema = StructType(Field("a", BigIntType))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType(
        "eel_schema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, Repetition.OPTIONAL).precision(38).scale(0).id(1).length(20).as(OriginalType.DECIMAL).named("a")
      )
  }

  it should "store char as BINARY with UTF8" in {
    val schema = StructType(Field("a", CharType(255)))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType("eel_schema", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "a", OriginalType.UTF8))
  }

  it should "store varchar as BINARY with UTF8" in {
    val schema = StructType(Field("a", VarcharType(255)))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType("eel_schema", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "a", OriginalType.UTF8))
  }

  it should "store times as INT32 with original type tag TIME_MILLIS" in {
    val schema = StructType(Field("a", TimeMillisType))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType("eel_schema", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "a", OriginalType.TIME_MILLIS))
  }

  it should "store doubles as DOUBLE" in {
    val schema = StructType(Field("a", DoubleType))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType("eel_schema", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "a"))
  }

  it should "store booleans as BOOLEAN" in {
    val schema = StructType(Field("a", BooleanType))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType("eel_schema", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BOOLEAN, "a"))
  }

  it should "store floats as FLOAT" in {
    val schema = StructType(Field("a", FloatType))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType("eel_schema", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.FLOAT, "a"))
  }

  it should "store signed shorts as INT32 with original type INT_16" in {
    val schema = StructType(Field("a", ShortType(true)))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType("eel_schema", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "a", OriginalType.INT_16))
  }

  it should "store unsigned shorts as INT32 with unsigned original type UINT_16" in {
    val schema = StructType(Field("a", ShortType(false)))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType("eel_schema", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "a", OriginalType.UINT_16))
  }

  it should "store signed ints as INT32" in {
    val schema = StructType(Field("a", IntType(true)))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType("eel_schema", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "a"))
  }

  it should "store unsigned ints as INT32 with unsigned original type UINT_32" in {
    val schema = StructType(Field("a", IntType(false)))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType("eel_schema", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "a", OriginalType.UINT_32))
  }

  it should "store signed longs as INT64" in {
    val schema = StructType(Field("a", LongType(true)))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType("eel_schema", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, "a"))
  }

  it should "store unsigned longs as INT64 with unsigned original type" in {
    val schema = StructType(Field("a", LongType(false)))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType("eel_schema", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, "a", OriginalType.UINT_64))
  }

  it should "store dates as int32 with original type tag DATE" in {
    val schema = StructType(Field("a", DateType))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType("eel_schema", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "a", OriginalType.DATE))
  }

  it should "store Strings as Binary with original type tag UTF8" in {
    val schema = StructType(Field("a", StringType))
    ParquetSchemaFns.toParquetMessageType(schema) shouldBe
      new MessageType("eel_schema", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "a", OriginalType.UTF8))
  }

  it should "read optional LIST types as nullable arrays" in {
    val messageType = new MessageType(
      "eel_schema",
      new GroupType(Repetition.OPTIONAL, "a", OriginalType.LIST,
        new GroupType(Repetition.REPEATED, "list",
          new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "element")
        )
      )
    )
    ParquetSchemaFns.fromParquetMessageType(messageType) shouldBe StructType(
      Field("a", ArrayType(DoubleType), nullable = true)
    )
  }

  it should "read required LIST types as non-null arrays" in {
    val messageType = new MessageType(
      "eel_schema",
      new GroupType(Repetition.REQUIRED, "a", OriginalType.LIST,
        new GroupType(Repetition.REPEATED, "list",
          new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "element")
        )
      )
    )
    ParquetSchemaFns.fromParquetMessageType(messageType) shouldBe StructType(
      Field("a", ArrayType(DoubleType), nullable = false)
    )
  }

  it should "read repeated groups as non-nullable array" in {
    val messageType = new MessageType(
      "eel_schema",
      new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "word", OriginalType.UTF8),
      new PrimitiveType(Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.DOUBLE, "items", OriginalType.LIST)
    )
    ParquetSchemaFns.fromParquetMessageType(messageType) shouldBe StructType(
      Field("word", BooleanType, nullable = true),
      Field("items", ArrayType(DoubleType), nullable = false)
    )
  }

  it should "store nullable array type as a 3-level optional LIST type" in {

    val structType = StructType(
      Field("b", ArrayType(BooleanType), nullable = true)
    )

    ParquetSchemaFns.toParquetMessageType(structType, "eel_schema") shouldBe new MessageType(
      "eel_schema",
      new GroupType(Repetition.OPTIONAL, "b", OriginalType.LIST,
        new GroupType(Repetition.REPEATED, "list",
          new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BOOLEAN, "element")
        )
      )
    )
  }

  it should "store non-null array type as a 3-level required LIST type" in {

    val structType = StructType(
      Field("b", ArrayType(BooleanType), nullable = false)
    )

    ParquetSchemaFns.toParquetMessageType(structType, "eel_schema") shouldBe new MessageType(
      "eel_schema",
      new GroupType(Repetition.REQUIRED, "b", OriginalType.LIST,
        new GroupType(Repetition.REPEATED, "list",
          new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BOOLEAN, "element")
        )
      )
    )
  }

  it should "read repeated primitive as non-nullable array" in {
    val messageType = new MessageType(
      "eel_schema",
      new PrimitiveType(Repetition.REPEATED, PrimitiveType.PrimitiveTypeName.DOUBLE, "doubles")
    )
    ParquetSchemaFns.fromParquetMessageType(messageType) shouldBe StructType(
      Field("doubles", ArrayType(DoubleType), nullable = false)
    )
  }

  it should "read repeated group as non-nullable array of structs" in {
    val messageType = new MessageType(
      "eel_schema",
      new GroupType(Repetition.REPEATED, "structs",
        new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, "a"),
        new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "b")
      )
    )
    ParquetSchemaFns.fromParquetType(messageType) shouldBe StructType(
      Field("structs", ArrayType(StructType(Field("a", FloatType, true), Field("b", DoubleType, false))), nullable = false)
    )
  }

  it should "store nullable map type as a required 3-level structure annotated with MAP" in {

    val structType = StructType(
      Field("abc", MapType(IntType.Signed, BooleanType), nullable = true)
    )

    ParquetSchemaFns.toParquetMessageType(structType, "eel_schema") shouldBe new MessageType(
      "eel_schema",
      new GroupType(Repetition.OPTIONAL, "abc", OriginalType.MAP,
        new GroupType(Repetition.REPEATED, "map",
          new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "key"),
          new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "value")
        )
      )
    )
  }

  it should "store non-null map type as an required 3-level structure annotated with MAP" in {

    val structType = StructType(
      Field("abc", MapType(IntType.Signed, BooleanType), nullable = false)
    )

    ParquetSchemaFns.toParquetMessageType(structType, "eel_schema") shouldBe new MessageType(
      "eel_schema",
      new GroupType(Repetition.REQUIRED, "abc", OriginalType.MAP,
        new GroupType(Repetition.REPEATED, "map",
          new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "key"),
          new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "value")
        )
      )
    )
  }

  it should "read optional group annotated with MAP" in {

    val messageType = new MessageType(
      "eel_schema",
      new GroupType(Repetition.OPTIONAL, "abc", OriginalType.MAP,
        new GroupType(Repetition.REPEATED, "key_value",
          new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FLOAT, "key"),
          new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "value")
        )
      )
    )

    ParquetSchemaFns.fromParquetType(messageType) shouldBe StructType(
      Field("abc", MapType(FloatType, DoubleType), nullable = true)
    )
  }

  it should "read required group annotated with MAP" in {

    val messageType = new MessageType(
      "eel_schema",
      new GroupType(Repetition.REQUIRED, "abc", OriginalType.MAP,
        new GroupType(Repetition.REPEATED, "key_value",
          new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FLOAT, "key"),
          new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "value")
        )
      )
    )

    ParquetSchemaFns.fromParquetType(messageType) shouldBe StructType(
      Field("abc", MapType(FloatType, DoubleType), nullable = false)
    )
  }

  it should "support map types that don't follow the naming convention" in {
    val messageType = new MessageType(
      "eel_schema",
      new GroupType(Repetition.REQUIRED, "a", OriginalType.MAP,
        new GroupType(Repetition.REPEATED, "b",
          new PrimitiveType(Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FLOAT, "c"),
          new PrimitiveType(Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "d")
        )
      )
    )

    ParquetSchemaFns.fromParquetType(messageType) shouldBe StructType(
      Field("a", MapType(FloatType, DoubleType), nullable = false)
    )
  }

  "ParquetSchemaFns.byteSizeForPrecision" should "calculate fixed length bytes required from precision" in {
    ParquetSchemaFns.byteSizeForPrecision(38) shouldBe 16
  }
}