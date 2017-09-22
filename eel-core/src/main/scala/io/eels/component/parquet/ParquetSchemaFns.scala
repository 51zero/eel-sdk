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

  implicit class RichType(tpe: Type) {
    def isGroupType: Boolean = !tpe.isPrimitive
  }

  def fromParquetPrimitiveType(tpe: PrimitiveType): DataType = {
    val baseType = tpe.getPrimitiveTypeName match {
      // we have no way of knowing if this was meant to be a varchar or char, but varchar seems more common
      // so we'll go for that
      case PrimitiveTypeName.BINARY if tpe.getOriginalType == OriginalType.UTF8 && tpe.getTypeLength > 0 =>
        VarcharType(tpe.getTypeLength)
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
      case PrimitiveTypeName.INT64 if tpe.getOriginalType == OriginalType.UINT_64 => LongType.Unsigned
   //   case PrimitiveTypeName.INT64 if tpe.getOriginalType == OriginalType.TIME_MICROS => TimeMicrosType
      case PrimitiveTypeName.INT64 if tpe.getOriginalType == OriginalType.TIMESTAMP_MILLIS => TimestampMillisType
   //   case PrimitiveTypeName.INT64 if tpe.getOriginalType == OriginalType.TIMESTAMP_MICROS => TimestampMicrosType
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
    } else if (tpe.getOriginalType == OriginalType.MAP) {
      fromParquetMapType(tpe.asGroupType)
    } else if (tpe.getOriginalType == OriginalType.LIST) {
      fromParquetArrayType(tpe.asGroupType)
    } else {
      fromParquetGroupType(tpe.asGroupType)
    }
  }

  def fromParquetArrayType(gt: GroupType): ArrayType = {
    val elementType = fromParquetType(gt.getFields.get(0).asGroupType().getFields.get(0))
    ArrayType(elementType)
  }

  // if the parquet group has just two fields, key and value, then we assume its a map
  def fromParquetMapType(gt: GroupType): MapType = {
    val key_value = gt.getFields.get(0).asGroupType()
    val keyType = fromParquetType(key_value.getFields.get(0))
    val valueType = fromParquetType(key_value.getFields.get(1))
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
      case StructType(fields) => Types.buildGroup(repetition).addFields(fields.map(toParquetType): _*).named(name)
      // arrays are written out in the style of spark, which is an outer, optional group,
      // marked with original type List, and the name of the real field. This group then contains
      // a single field, which is a repeated group, name of 'list', and no original type.
      // This then contains a single field called element, which is an optional group,
      // no original type, and fields taken from our array's element type.
      case ArrayType(elementType) =>
        val listType = toParquetType(elementType, "element", false)
        Types.list(repetition)
          .element(listType)
          .named(name)
      case BigIntType =>
        Types.primitive(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
          .precision(38)
          .scale(0)
          .as(OriginalType.DECIMAL)
          .length(20)
          .id(1)
          .named(name)
      case BinaryType => Types.primitive(PrimitiveTypeName.BINARY, repetition).named(name)
      case BooleanType => Types.primitive(PrimitiveTypeName.BOOLEAN, repetition).named(name)
      case ByteType(true) => Types.primitive(PrimitiveTypeName.INT32, repetition).as(OriginalType.INT_8).named(name)
      case ByteType(false) => Types.primitive(PrimitiveTypeName.INT32, repetition).as(OriginalType.UINT_8).named(name)
      case CharType(length) => Types.primitive(PrimitiveTypeName.BINARY, repetition).as(OriginalType.UTF8).length(length).named(name)
      case DateType => Types.primitive(PrimitiveTypeName.INT32, repetition).as(OriginalType.DATE).named(name)
      // https://github.com/Parquet/parquet-format/blob/master/LogicalTypes.md#decimal
      // The scale stores the number of digits of that value that are to the right of the decimal point,
      // and the precision stores the maximum number of digits supported in the unscaled value.
      case DecimalType(precision, scale) =>
        val byteSize = byteSizeForPrecision(precision)
        Types.primitive(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
          .precision(precision.value)
          .scale(scale.value)
          .as(OriginalType.DECIMAL)
          .length(byteSize)
          .id(1)
          .named(name)
      case DoubleType => Types.primitive(PrimitiveTypeName.DOUBLE, repetition).named(name)
      case EnumType(enumName, _) => Types.primitive(PrimitiveTypeName.BINARY, repetition).as(OriginalType.ENUM).named(enumName)
      case FloatType => Types.primitive(PrimitiveTypeName.FLOAT, repetition).named(name)
      case IntType(true) => Types.primitive(PrimitiveTypeName.INT32, repetition).named(name)
      case IntType(false) => Types.primitive(PrimitiveTypeName.INT32, repetition).as(OriginalType.UINT_32).named(name)
      case LongType(true) => Types.primitive(PrimitiveTypeName.INT64, repetition).named(name)
      case LongType(false) => Types.primitive(PrimitiveTypeName.INT64, repetition).as(OriginalType.UINT_64).named(name)
      case MapType(keyType, valueType) =>
        val key = toParquetType(keyType, "key", false)
        val value = toParquetType(valueType, "value", true)
        Types.map(repetition).key(key).value(value).named(name)
      case ShortType(true) => Types.primitive(PrimitiveTypeName.INT32, repetition).as(OriginalType.INT_16).named(name)
      case ShortType(false) => Types.primitive(PrimitiveTypeName.INT32, repetition).as(OriginalType.UINT_16).named(name)
      // careful, a string type in parquet with a length set cannot be read in hive, so never set a length with a string here
      case StringType => Types.primitive(PrimitiveTypeName.BINARY, repetition).as(OriginalType.UTF8).named(name)
      // this is just the time of the day, no date component. not the same as a timestamp!
      case TimeMillisType => Types.primitive(PrimitiveTypeName.INT32, repetition).as(OriginalType.TIME_MILLIS).named(name)
   //   case TimeMicrosType => new PrimitiveType(repetition, PrimitiveTypeName.INT64, name, OriginalType.TIME_MICROS)
      // spark doesn't annotate timestamps, just uses int96?
      case TimestampMillisType => Types.primitive(PrimitiveTypeName.INT96, repetition).named(name)
   //   case TimestampMicrosType => new PrimitiveType(repetition, PrimitiveTypeName.INT64, name, OriginalType.TIMESTAMP_MICROS)
      case VarcharType(length) => Types.primitive(PrimitiveTypeName.BINARY, repetition).as(OriginalType.UTF8).length(length).named(name)
    }
  }
}
