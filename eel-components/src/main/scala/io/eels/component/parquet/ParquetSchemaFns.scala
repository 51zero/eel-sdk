package io.eels.component.parquet

import io.eels.schema._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._

object ParquetSchemaFns {

  def toParquetType(field: Field): Type = {
    val repetition = if (field.nullable) Repetition.OPTIONAL else Repetition.REQUIRED
    field.dataType match {
      case BigIntType => new PrimitiveType(repetition, PrimitiveTypeName.INT64, field.name)
      case BooleanType => new PrimitiveType(repetition, PrimitiveTypeName.BOOLEAN, field.name)
      case DateType => new PrimitiveType(repetition, PrimitiveTypeName.BINARY, field.name)
      case DecimalType(precision, scale) => new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, field.name, OriginalType.DECIMAL)
      case DoubleType => new PrimitiveType(repetition, PrimitiveTypeName.DOUBLE, field.name)
      case FloatType => new PrimitiveType(repetition, PrimitiveTypeName.FLOAT, field.name)
      case i: IntType => new PrimitiveType(repetition, PrimitiveTypeName.INT32, field.name)
      case l: LongType => new PrimitiveType(repetition, PrimitiveTypeName.INT64, field.name)
      case ShortType => new PrimitiveType(repetition, PrimitiveTypeName.INT32, field.name)
      case StructType(fields) => new GroupType(repetition, field.name, fields.map(toParquetType): _*)
      case StringType => new PrimitiveType(repetition, PrimitiveTypeName.BINARY, field.name, OriginalType.UTF8)
      case TimestampType => new PrimitiveType(repetition, PrimitiveTypeName.INT96, field.name, OriginalType.TIMESTAMP_MILLIS)
    }
  }

  def toParquetSchema(schema: StructType): MessageType = {
    val types = schema.fields.map(toParquetType)
    new MessageType("row", types: _*)
  }
}
