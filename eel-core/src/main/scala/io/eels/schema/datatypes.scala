package io.eels.schema

trait DataType

object StringType extends DataType
object BooleanType extends DataType
object BytesType extends DataType
object TimestampType extends DataType
object ShortType extends DataType
object FloatType extends DataType
object DoubleType extends DataType
object DateType extends DataType
object BinaryType extends DataType
object BigIntType extends DataType

case class IntType(signed: Boolean = false) extends DataType
case class LongType(signed: Boolean = false) extends DataType

case class CharType(size: Int) extends DataType
case class VarcharType(size: Int) extends DataType

case class DecimalType(scale: Scale = Scale(0),
                       precision: Precision = Precision(0)
                      ) extends DataType


case class ArrayType(elementType: DataType) extends DataType

case class Precision(value: Int) extends AnyVal
case class Scale(value: Int) extends AnyVal