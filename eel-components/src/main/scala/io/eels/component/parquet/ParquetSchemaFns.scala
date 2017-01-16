package io.eels.component.parquet

import io.eels.schema._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._

import scala.collection.JavaConverters._

/**
  * See parquet formats at https://github.com/Parquet/parquet-format/blob/master/LogicalTypes.md
  */
object ParquetSchemaFns {

  def fromParquetPrimitiveType(`type`: PrimitiveType): DataType = {
    `type`.getPrimitiveTypeName match {
      case PrimitiveTypeName.BINARY =>
        `type`.getOriginalType match {
          case OriginalType.ENUM => EnumType(`type`.getName, Nil)
          case OriginalType.UTF8 => StringType
          case _ => BinaryType
        }
      case PrimitiveTypeName.BOOLEAN => BooleanType
      case PrimitiveTypeName.DOUBLE => DoubleType
      case PrimitiveTypeName.FLOAT => FloatType
      case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY =>
        `type`.getOriginalType match {
          case OriginalType.DECIMAL =>
            val meta = `type`.getDecimalMetadata
            DecimalType(Precision(meta.getPrecision), Scale(meta.getScale))
          case _ => BinaryType
        }
      case PrimitiveTypeName.INT32 =>
        `type`.getOriginalType match {
          case OriginalType.UINT_32 => IntType.Unsigned
          case OriginalType.UINT_16 => ShortType.Unsigned
          case OriginalType.UINT_8 => ShortType.Unsigned
          case OriginalType.INT_16 => ShortType.Signed
          case OriginalType.INT_8 => ShortType.Signed
          case OriginalType.TIME_MILLIS => TimeMillisType
          case OriginalType.DATE => DateType
          case OriginalType.DECIMAL =>
            val meta = `type`.getDecimalMetadata
            DecimalType(Precision(meta.getPrecision), Scale(meta.getScale))
          case _ => IntType.Signed
        }
      case PrimitiveTypeName.INT64 if `type`.getOriginalType == OriginalType.UINT_64 => IntType.Unsigned
      case PrimitiveTypeName.INT64 if `type`.getOriginalType == OriginalType.TIME_MICROS => TimeMicrosType
      case PrimitiveTypeName.INT64 if `type`.getOriginalType == OriginalType.TIMESTAMP_MILLIS => TimestampMillisType
      case PrimitiveTypeName.INT64 if `type`.getOriginalType == OriginalType.TIMESTAMP_MICROS => TimestampMicrosType
      case PrimitiveTypeName.INT64 if `type`.getOriginalType == OriginalType.DECIMAL => DecimalType(Precision(18), Scale(2))
      case PrimitiveTypeName.INT64 => LongType.Signed
      // https://github.com/Parquet/parquet-mr/issues/218
      case PrimitiveTypeName.INT96 => TimestampMillisType
      case other => sys.error("Unsupported type " + other)
    }
  }

  def fromParquetGroupType(gt: GroupType): StructType = {
    val fields = gt.getFields.asScala.map { field =>
      val datatype = field.isPrimitive match {
        case true => fromParquetPrimitiveType(field.asPrimitiveType())
        case false => fromParquetGroupType(field.asGroupType)
      }
      field.getRepetition match {
        case Repetition.REPEATED => Field(field.getName, ArrayType(datatype), true)
        case _ => Field(field.getName, datatype, field.getRepetition == Repetition.OPTIONAL)
      }
    }
    StructType(fields)
  }

  def byteSizeForPrecision(precision: Precision): Int = {
    var fixedArrayLength = 0
    var base10Digits = 0
    while (base10Digits < precision.value) {
      fixedArrayLength = fixedArrayLength + 1
      base10Digits = Math.floor(Math.log10(Math.pow(2, 8 * fixedArrayLength - 1) - 1)).toInt
    }
    fixedArrayLength
  }

  def toParquetType(field: Field): Type = toParquetType(
    field.dataType,
    field.name,
    if (field.nullable) Repetition.OPTIONAL else Repetition.REQUIRED
  )

  def toParquetType(dataType: DataType, name: String, repetition: Repetition): Type = {
    dataType match {
      case StructType(fields) => new GroupType(repetition, name, fields.map(toParquetType): _*)
      case ArrayType(elementType) => toParquetType(elementType, name, Repetition.REPEATED)
      case BigIntType =>
        val id = new Type.ID(1)
        val metadata = new DecimalMetadata(38, 0)
        new PrimitiveType(repetition, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 20, name, OriginalType.DECIMAL, metadata, id)
      case BinaryType => new PrimitiveType(repetition, PrimitiveTypeName.BINARY, name)
      case BooleanType => new PrimitiveType(repetition, PrimitiveTypeName.BOOLEAN, name)
      case CharType(size) => new PrimitiveType(repetition, PrimitiveTypeName.BINARY, name, OriginalType.UTF8)
      case DateType => new PrimitiveType(repetition, PrimitiveTypeName.INT32, name, OriginalType.DATE)
      // https://github.com/Parquet/parquet-format/blob/master/LogicalTypes.md#decimal
      // The scale stores the number of digits of that value that are to the right of the decimal point,
      // and the precision stores the maximum number of digits supported in the unscaled value.
      case DecimalType(precision, scale) =>
        val metadata = new DecimalMetadata(precision.value, scale.value)
        val byteSize = byteSizeForPrecision(precision)
        val id = new Type.ID(1)
        new PrimitiveType(repetition, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, byteSize, name, OriginalType.DECIMAL, metadata, id)
      case DoubleType => new PrimitiveType(repetition, PrimitiveTypeName.DOUBLE, name)
      case EnumType(enumName, values) => new PrimitiveType(repetition, PrimitiveTypeName.BINARY, enumName, OriginalType.ENUM)
      case FloatType => new PrimitiveType(repetition, PrimitiveTypeName.FLOAT, name)
      case IntType(true) => new PrimitiveType(repetition, PrimitiveTypeName.INT32, name)
      case IntType(false) => new PrimitiveType(repetition, PrimitiveTypeName.INT32, name, OriginalType.UINT_32)
      case LongType(true) => new PrimitiveType(repetition, PrimitiveTypeName.INT64, name)
      case LongType(false) => new PrimitiveType(repetition, PrimitiveTypeName.INT64, name, OriginalType.UINT_64)
      case ShortType(true) => new PrimitiveType(repetition, PrimitiveTypeName.INT32, name, OriginalType.INT_16)
      case ShortType(false) => new PrimitiveType(repetition, PrimitiveTypeName.INT32, name, OriginalType.UINT_16)
      case StringType => new PrimitiveType(repetition, PrimitiveTypeName.BINARY, name, OriginalType.UTF8)
      case TimeMillisType => new PrimitiveType(repetition, PrimitiveTypeName.INT32, name, OriginalType.TIME_MILLIS)
      case TimeMicrosType => new PrimitiveType(repetition, PrimitiveTypeName.INT64, name, OriginalType.TIME_MICROS)
      // spark doesn't annotate timestamps, just uses int96, so same here
      case TimestampMillisType => new PrimitiveType(repetition, PrimitiveTypeName.INT96, name)
      case TimestampMicrosType => new PrimitiveType(repetition, PrimitiveTypeName.INT64, name, OriginalType.TIMESTAMP_MICROS)
      case VarcharType(size) => new PrimitiveType(repetition, PrimitiveTypeName.BINARY, name, OriginalType.UTF8)
    }
  }

  def toParquetSchema(schema: StructType, name: String = "row"): MessageType = {
    val types = schema.fields.map(toParquetType)
    new MessageType(name, types: _*)
  }
}
