package io.eels.component.orc

import io.eels.schema._
import org.apache.hadoop.hive.ql.exec.vector._

sealed trait OrcDeserializer[T <: ColumnVector] {
  def readFromVector(rowIndex: Int, vector: T): Any
}

object OrcDeserializer {
  def apply(dataType: DataType): OrcDeserializer[_ <: ColumnVector] = dataType match {
    case BooleanType => BooleanDeserializer
    case DateType => LongDeserializer
    case DecimalType(p, s) => DecimalDeserializer
    case DoubleType => DoubleDeserializer
    case FloatType => FloatDeserializer
    case IntType(_) => IntDeserializer
    case LongType(_) => LongDeserializer
    case ShortType(_) => IntDeserializer
    case StringType => StringDeserializer
    case StructType(fields) => new StructDeserializer(fields, fields.zipWithIndex.map(_._2))
    case TimestampMillisType => TimestampDeserializer
  }
}

class StructDeserializer(fields: Seq[Field], projectionColumns: Seq[Int]) extends OrcDeserializer[StructColumnVector] {
  require(fields.size == projectionColumns.size, "Struct should receive the fields that match the projection")

  private val deserializers = fields.map(_.dataType).map(OrcDeserializer.apply).toArray

  def readFromColumn[T <: ColumnVector](rowIndex: Int, k: Int, struct: StructColumnVector): Any = {
    val deser = deserializers(k).asInstanceOf[OrcDeserializer[T]]
    val colIndex = projectionColumns(k)
    val vector = struct.fields(colIndex).asInstanceOf[T]
    deser.readFromVector(rowIndex, vector)
  }

  override def readFromVector(rowIndex: Int, vector: StructColumnVector): Vector[Any] = {
    val builder = Vector.newBuilder[Any]
    builder.sizeHint(projectionColumns.length)
    for (k <- projectionColumns.indices) {
      builder += readFromColumn(rowIndex, k, vector)
    }
    builder.result
  }
}

object TimestampDeserializer extends OrcDeserializer[TimestampColumnVector] {
  override def readFromVector(rowIndex: Int, vector: TimestampColumnVector): java.sql.Timestamp = {
    if (vector.isNull(rowIndex)) null
    else new java.sql.Timestamp(vector.getTime(rowIndex))
  }
}

object DecimalDeserializer extends OrcDeserializer[DecimalColumnVector] {
  override def readFromVector(rowIndex: Int, vector: DecimalColumnVector): BigDecimal = {
    if (vector.isNull(rowIndex)) null
    else BigDecimal(vector.vector(rowIndex).getHiveDecimal.bigDecimalValue)
  }
}

object StringDeserializer extends OrcDeserializer[BytesColumnVector] {
  override def readFromVector(rowIndex: Int, vector: BytesColumnVector): Any = {
    if (vector.isNull(rowIndex)) {
      null
    } else {
      val bytes = vector.vector.head.slice(vector.start(rowIndex), vector.start(rowIndex) + vector.length(rowIndex))
      new String(bytes, "UTF8")
    }
  }
}

object IntDeserializer extends OrcDeserializer[LongColumnVector] {
  override def readFromVector(rowIndex: Int, vector: LongColumnVector): Any = {
    if (vector.isNull(rowIndex)) null
    else vector.vector(rowIndex).toInt
  }
}

object DoubleDeserializer extends OrcDeserializer[DoubleColumnVector] {
  override def readFromVector(rowIndex: Int, vector: DoubleColumnVector): Any = {
    if (vector.isNull(rowIndex)) null
    else vector.vector(rowIndex)
  }
}

object FloatDeserializer extends OrcDeserializer[DoubleColumnVector] {
  override def readFromVector(rowIndex: Int, vector: DoubleColumnVector): Any = {
    if (vector.isNull(rowIndex)) null
    else vector.vector(rowIndex).toFloat
  }
}

object LongDeserializer extends OrcDeserializer[LongColumnVector] {
  override def readFromVector(rowIndex: Int, vector: LongColumnVector): Any = {
    if (vector.isNull(rowIndex)) null
    else vector.vector(rowIndex)
  }
}

object BooleanDeserializer extends OrcDeserializer[LongColumnVector] {
  override def readFromVector(rowIndex: Int, vector: LongColumnVector): Boolean = vector.vector(rowIndex) == 1
}
