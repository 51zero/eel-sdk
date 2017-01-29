package io.eels.component.parquet

import io.eels.schema.{StructType, _}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._

import scala.collection.JavaConverters._

/**
  * See parquet formats at https://github.com/Parquet/parquet-format/blob/master/LogicalTypes.md
  */
object ParquetSchemaFns {

  def fromParquetPrimitiveType(tpe: PrimitiveType): DataType = {
    val baseType = tpe.getPrimitiveTypeName match {
      case PrimitiveTypeName.BINARY =>
        tpe.getOriginalType match {
          case OriginalType.ENUM => EnumType(tpe.getName, Nil)
          case OriginalType.UTF8 => StringType
          case _ => BinaryType
        }
      case PrimitiveTypeName.BOOLEAN => BooleanType
      case PrimitiveTypeName.DOUBLE => DoubleType
      case PrimitiveTypeName.FLOAT => FloatType
      case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY =>
        tpe.getOriginalType match {
          case OriginalType.DECIMAL =>
            val meta = tpe.getDecimalMetadata
            DecimalType(Precision(meta.getPrecision), Scale(meta.getScale))
          case _ => BinaryType
        }
      case PrimitiveTypeName.INT32 =>
        tpe.getOriginalType match {
          case OriginalType.UINT_32 => IntType.Unsigned
          case OriginalType.UINT_16 => ShortType.Unsigned
          case OriginalType.UINT_8 => ShortType.Unsigned
          case OriginalType.INT_16 => ShortType.Signed
          case OriginalType.INT_8 => ShortType.Signed
          case OriginalType.TIME_MILLIS => TimeMillisType
          case OriginalType.DATE => DateType
          case OriginalType.DECIMAL =>
            val meta = tpe.getDecimalMetadata
            DecimalType(Precision(meta.getPrecision), Scale(meta.getScale))
          case _ => IntType.Signed
        }
      case PrimitiveTypeName.INT64 if tpe.getOriginalType == OriginalType.UINT_64 => IntType.Unsigned
      case PrimitiveTypeName.INT64 if tpe.getOriginalType == OriginalType.TIME_MICROS => TimeMicrosType
      case PrimitiveTypeName.INT64 if tpe.getOriginalType == OriginalType.TIMESTAMP_MILLIS => TimestampMillisType
      case PrimitiveTypeName.INT64 if tpe.getOriginalType == OriginalType.TIMESTAMP_MICROS => TimestampMicrosType
      case PrimitiveTypeName.INT64 if tpe.getOriginalType == OriginalType.DECIMAL => DecimalType(Precision(18), Scale(2))
      case PrimitiveTypeName.INT64 => LongType.Signed
      // https://github.com/Parquet/parquet-mr/issues/218
      case PrimitiveTypeName.INT96 => TimestampMillisType
      case other => sys.error("Unsupported type " + other)
    }
    if (tpe.isRepetition(Repetition.REPEATED)) ArrayType(baseType) else baseType
  }

  def fromParquetMessageType(messageType: MessageType): StructType = {
    val fields = messageType.getFields.asScala.map { tpe =>
      val dataType = fromParquetType(tpe)
      Field(tpe.getName, dataType, tpe.getRepetition == Repetition.OPTIONAL)
    }
    StructType(fields)
  }

  def fromParquetType(tpe: Type): DataType = {
    if (tpe.isPrimitive) {
      fromParquetPrimitiveType(tpe.asPrimitiveType)
      // a map must be a group, with key/value fields or tagged as map
    } else if (tpe.isInstanceOf[MessageType]) {
      fromParquetMessageType(tpe.asInstanceOf[MessageType])
    } else if (isMap(tpe)) {
      fromParquetMapType(tpe.asGroupType)
      // could be a nullable list - which is an optional group of a repeated group of a required element
    } else if (isNullableList(tpe)) {
      val elementType = fromParquetType(tpe.asGroupType().getFields.get(0).asGroupType().getFields.get(0))
      ArrayType(elementType)
    } else {
      fromParquetGroupType(tpe.asGroupType)
    }
  }

  // if the parquet group has just two fields, key and value, then we assume its a map
  def fromParquetMapType(gt: GroupType): MapType = {
    val keyType = fromParquetType(gt.getFields.get(0))
    val valueType = fromParquetType(gt.getFields.get(1))
    MapType(keyType, valueType)
  }

  def fromParquetGroupType(gt: GroupType): DataType = {
    val fields = gt.getFields.asScala.map { tpe =>
      val dataType = fromParquetType(tpe)
      Field(tpe.getName, dataType, tpe.getRepetition == Repetition.OPTIONAL)
    }
    val struct = StructType(fields)
    if (gt.isRepetition(Repetition.REPEATED)) ArrayType(struct) else struct
  }

  def isArray(pt: PrimitiveType): Boolean = pt.isRepetition(Repetition.REPEATED)
  def isArray(gt: GroupType): Boolean = gt.getOriginalType == OriginalType.LIST || gt.isRepetition(Repetition.REPEATED)

  def isMap(tpe: Type): Boolean = !tpe.isPrimitive && isMap(tpe.asGroupType)
  def isMap(gt: GroupType): Boolean = {
    gt.getOriginalType == OriginalType.MAP ||
      gt.getFieldCount == 2 && gt.containsField("key") && gt.containsField("value")
  }

  implicit class RichType(tpe: Type) {
    def isGroupType: Boolean = !tpe.isPrimitive
  }

  // spark style nullable list - which is an optional group of a repeated element
  def isNullableList(tpe: Type): Boolean = {
    tpe.getOriginalType == OriginalType.LIST && tpe.isGroupType && {
      val gt = tpe.asGroupType()
      gt.getFieldCount == 1 &&
        gt.getFields.get(0).isRepetition(Repetition.REPEATED) &&
        gt.getFields.get(0).getName == "list" &&
        gt.getFields.get(0).isGroupType && {
        val gt2 = gt.getFields.get(0).asGroupType()
        gt2.getFieldCount == 1 &&
          gt2.getFields.get(0).getName == "element" &&
          gt2.getFields.get(0).isRepetition(Repetition.REQUIRED)
      }
    }
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


  def toParquetMessageType(structType: StructType, name: String = "eel_schema"): MessageType = {
    val types = structType.fields.map(toParquetType)
    new MessageType(name, types: _*)
  }

  def toParquetType(field: Field): Type = toParquetType(field.dataType, field.name, field.nullable)
  def toParquetType(dataType: DataType, name: String, nullable: Boolean): Type = {
    val repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
    dataType match {
      case StructType(fields) => new GroupType(repetition, name, fields.map(toParquetType): _*)
      // nullable arrays should be written as spark style nested groups
      case ArrayType(elementType) if nullable =>
        new GroupType(Repetition.OPTIONAL, name, OriginalType.LIST, new GroupType(Repetition.REPEATED, "list", toParquetType(elementType, "element", false)))
      // non-nullable arrays can be written as repeated lists of required elements
      case ArrayType(elementType) =>
        toParquetType(elementType, name, false)
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
      case MapType(keyType, valueType) =>
        val key = toParquetType(keyType, "key", false)
        val value = toParquetType(valueType, "value", false)
        new GroupType(Repetition.REPEATED, name, OriginalType.MAP, key, value)
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
}
