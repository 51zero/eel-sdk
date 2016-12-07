package io.eels.component.parquet

import io.eels.schema._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._

object ParquetSchemaFns {

  def toParquetType(field: Field): Type = field.dataType match {
    case BigIntType => new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT96, field.name)
    case BooleanType => new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BOOLEAN, field.name)
    case DateType => new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, field.name)
    case DecimalType(precision, scale) => new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, field.name)
    case DoubleType => new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, field.name)
    case FloatType => new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.DOUBLE, field.name)
    case i: IntType => new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, field.name)
    case l: LongType => new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT64, field.name)
    case ShortType => new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT32, field.name)
    case StructType(fields) => new GroupType(Repetition.REQUIRED, field.name, fields.map(toParquetType): _*)
    case StringType => new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, field.name, OriginalType.UTF8)
    case TimestampType => new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT96, field.name, OriginalType.TIMESTAMP_MILLIS)
  }

  def toParquetSchema(schema: StructType): MessageType = {
    val types = schema.fields.map(toParquetType)
    new MessageType("row", types: _*)
  }
}
