package io.eels.component.orc

import io.eels.schema._
import org.apache.hadoop.hive.ql.exec.vector._

sealed trait OrcDeserializer[T <: ColumnVector] {
  def readFromVector(rowIndex: Int, vector: T): Any
}

object OrcDeserializer {
  def apply(dataType: DataType): OrcDeserializer[_ <: ColumnVector] = dataType match {
    case BooleanType => BooleanDeserializer
    case DecimalType(p, s) => DecimalDeserializer
    case DoubleType => DoubleDeserializer
    case FloatType => FloatDeserializer
    case IntType(_) => IntDeserializer
    case ShortType(_) => IntDeserializer
    case LongType(_) => LongDeserializer
    case StringType => StringDeserializer
    case TimestampMillisType => TimestampDeserializer
  }
}

object TimestampDeserializer extends OrcDeserializer[TimestampColumnVector] {
  override def readFromVector(rowIndex: Int, vector: TimestampColumnVector): java.sql.Timestamp = {
    new java.sql.Timestamp(vector.getTime(rowIndex))
  }
}

object DecimalDeserializer extends OrcDeserializer[DecimalColumnVector] {
  override def readFromVector(rowIndex: Int, vector: DecimalColumnVector): BigDecimal = {
    BigDecimal(vector.vector(rowIndex).getHiveDecimal.bigDecimalValue)
  }
}

object StringDeserializer extends OrcDeserializer[BytesColumnVector] {
  override def readFromVector(rowIndex: Int, vector: BytesColumnVector): Any = {
    val bytes = vector.vector.head.slice(vector.start(rowIndex), vector.start(rowIndex) + vector.length(rowIndex))
    new String(bytes, "UTF8")
  }
}

object IntDeserializer extends OrcDeserializer[LongColumnVector] {
  override def readFromVector(rowIndex: Int, vector: LongColumnVector): Int = vector.vector(rowIndex).toInt
}

object DoubleDeserializer extends OrcDeserializer[DoubleColumnVector] {
  override def readFromVector(rowIndex: Int, vector: DoubleColumnVector): Double = vector.vector(rowIndex)
}

object FloatDeserializer extends OrcDeserializer[DoubleColumnVector] {
  override def readFromVector(rowIndex: Int, vector: DoubleColumnVector): Double = vector.vector(rowIndex).toFloat
}

object LongDeserializer extends OrcDeserializer[LongColumnVector] {
  override def readFromVector(rowIndex: Int, vector: LongColumnVector): Long = vector.vector(rowIndex)
}

object BooleanDeserializer extends OrcDeserializer[LongColumnVector] {
  override def readFromVector(rowIndex: Int, vector: LongColumnVector): Boolean = vector.vector(rowIndex) == 1
}
