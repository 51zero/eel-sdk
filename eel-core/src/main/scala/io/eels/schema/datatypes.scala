package io.eels.schema

trait DataType {
  def canonicalName: String = getClass.getSimpleName.toLowerCase.stripSuffix("type")
  def matches(from: DataType): Boolean = this == from
}

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

case class IntType(signed: Boolean = true) extends DataType
case class LongType(signed: Boolean = true) extends DataType

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
}

case class ArrayType(elementType: DataType) extends DataType {
  override def canonicalName: String = "array<" + elementType.canonicalName + ">"
}

case class Precision(value: Int) extends AnyVal
case class Scale(value: Int) extends AnyVal