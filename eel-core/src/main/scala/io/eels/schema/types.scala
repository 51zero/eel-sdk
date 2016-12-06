package io.eels.schema

trait DataType {
  def canonicalName: String = getClass.getSimpleName.toLowerCase.stripSuffix("type")
  def matches(from: DataType): Boolean = this == from
}

object BigIntType extends DataType
object BinaryType extends DataType
object BooleanType extends DataType
object BytesType extends DataType
object DateType extends DataType
object DoubleType extends DataType
object FloatType extends DataType
object ShortType extends DataType
object StringType extends DataType
object TimestampType extends DataType

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

case class CharType(size: Int) extends DataType
case class VarcharType(size: Int) extends DataType

case class DecimalType(scale: Scale = Scale(0),
                       precision: Precision = Precision(0)
                      ) extends DataType {
  override def canonicalName: String = "decimal(" + precision.value + "," + scale.value + ")"
  override def matches(from: DataType) = from match {
    case DecimalType(s, p) => (s == scale || s.value == -1 || scale.value == -1) && (p == precision || p.value == -1 || precision.value == -1)
    case other => false
  }
}

object DecimalType {
  val Wildcard = DecimalType(Scale(-1), Precision(-1))
  val Default = DecimalType(Scale(18), Precision(18))
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
case class Scale(value: Int) extends AnyVal