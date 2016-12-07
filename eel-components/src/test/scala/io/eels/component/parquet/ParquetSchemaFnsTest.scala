package io.eels.component.parquet

import io.eels.schema._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, OriginalType, PrimitiveType}
import org.scalatest.{FlatSpec, Matchers}

class ParquetSchemaFnsTest extends FlatSpec with Matchers {

  "ParquetSchemaFns.toParquetSchema" should "support timestamps to int96" in {
    val schema = StructType(Field("a", TimestampType))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT96, "a", OriginalType.TIMESTAMP_MILLIS))
  }

  it should "support dates to Binary" in {
    val schema = StructType(Field("a", DateType))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "a"))
  }

  it should "support Strings to Binary with UTF8" in {
    val schema = StructType(Field("a", StringType))
    ParquetSchemaFns.toParquetSchema(schema) shouldBe
      new MessageType("row", new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "a", OriginalType.UTF8))
  }
}
