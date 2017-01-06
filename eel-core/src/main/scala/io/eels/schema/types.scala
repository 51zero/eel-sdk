package io.eels.schema

import scala.language.implicitConversions

trait DataType {
  def canonicalName: String = getClass.getSimpleName.toLowerCase.stripSuffix("$").stripSuffix("type")
  def matches(from: DataType): Boolean = this == from
}

object BigIntType extends DataType
object BinaryType extends DataType
object BooleanType extends DataType
object DateType extends DataType
object DoubleType extends DataType
object FloatType extends DataType
object StringType extends DataType

// a time without a date; number of milliseconds after midnight
object TimeMillisType extends DataType

// a time without a date; number of micros after midnight
object TimeMicrosType extends DataType

// number of millis since the unix epoch for UTC
object TimestampMillisType extends DataType

// number of micros since the unix epoch for UTC
object TimestampMicrosType extends DataType

case class EnumType(name: String, values: Seq[String]) extends DataType
object EnumType {
  def apply(name: String, first: String, rest: String*): EnumType = new EnumType(name, first +: rest)
}

case class ShortType(signed: Boolean = true) extends DataType

object ShortType {
  val Signed = ShortType(true)
  val Unsigned = ShortType(false)
}

case class IntType(signed: Boolean = true) extends DataType

object IntType {
  val Signed = IntType(true)
  val Unsigned = IntType(false)
}

case class LongType(signed: Boolean = true) extends DataType

object LongType {
  val Signed = LongType(true)
  val Unsigned = LongType(false)
}

case class CharType(size: Int) extends DataType {
  override def canonicalName: String = s"char($size)"
}

case class VarcharType(size: Int) extends DataType {
  override def canonicalName: String = s"varchar($size)"
}

case class DecimalType(precision: Precision = Precision(0),
                       scale: Scale = Scale(0)) extends DataType {
  if (precision.value != -1)
    require(scale.value <= precision.value, s"Scale ${scale.value} should be less than or equal to precision ${precision.value}")
  override def canonicalName: String = "decimal(" + precision.value + "," + scale.value + ")"
  override def matches(from: DataType) = from match {
    case DecimalType(p, s) => (s == scale || s.value == -1 || scale.value == -1) && (p == precision || p.value == -1 || precision.value == -1)
    case other => false
  }
}

object DecimalType {
  val Wildcard = DecimalType(Precision(-1), Scale(-1))
  val Default = DecimalType(Precision(18), Scale(2))
}

case class ArrayType(elementType: DataType) extends DataType {
  override def canonicalName: String = "array<" + elementType.canonicalName + ">"
}

object ArrayType {

  val Doubles = ArrayType(DoubleType)
  val SignedInts = ArrayType(IntType.Signed)
  val SignedLongs = ArrayType(LongType.Signed)
  val Booleans = ArrayType(BooleanType)
  val Strings = ArrayType(StringType)

  def cached(elementType: DataType) : ArrayType = elementType match {
    case DoubleType => ArrayType.Doubles
    case IntType.Signed => ArrayType.SignedInts
    case LongType.Signed => ArrayType.SignedLongs
    case BooleanType => ArrayType.Booleans
    case StringType => ArrayType.Strings
    case _ => ArrayType(elementType)
  }
}

case class Precision(value: Int) extends AnyVal
object Precision {
  implicit def intToPrecision(value: Int): Precision = Precision(value)
}

case class Scale(value: Int) extends AnyVal
object Scale {
  implicit def intToScale(value: Int): Scale = Scale(value)
}