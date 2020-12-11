package io.eels.component.orc

import io.eels.schema._
import org.apache.hadoop.hive.ql.exec.vector._

sealed trait OrcDeserializer[T <: ColumnVector] {
  def readFromVector(rowIndex: Int, vector: T): Any
}

object OrcDeserializer {
  def apply(dataType: DataType): OrcDeserializer[_ <: ColumnVector] = dataType match {
    case ArrayType(elementType) => new ListDeserializer(apply(elementType))
    case BooleanType => BooleanDeserializer
    case CharType(size) => StringDeserializer
    case DateType => DateDeserializer
    case DecimalType(p, s) => DecimalDeserializer
    case DoubleType => DoubleDeserializer
    case FloatType => FloatDeserializer
    case IntType(_) => IntDeserializer
    case LongType(_) => LongDeserializer
    case MapType(keyType, valueType) => new MapDeserializer(apply(keyType), apply(valueType))
    case ShortType(_) => IntDeserializer
    case StringType => StringDeserializer
    case StructType(fields) => new StructDeserializer(fields, fields.zipWithIndex.map(_._2))
    case TimestampMillisType => TimestampDeserializer
    case VarcharType(size) => StringDeserializer
  }
}

class ListDeserializer[T <: ColumnVector](nested: OrcDeserializer[T]) extends OrcDeserializer[ListColumnVector] {
  override def readFromVector(rowIndex: Int, vector: ListColumnVector): Any = {
    // the row index is just a pointer into the list column vector offsets array to get the real pointer
    // the lengths will tell us how many to read from that point
    val offset = vector.offsets(rowIndex).toInt
    val length = vector.lengths(rowIndex).toInt
    val values = for (k <- offset until offset + length) yield {
      if (!vector.noNulls && vector.isNull(k)) null
      else if (!vector.noNulls && vector.isRepeating && vector.isNull(0)) null
      else nested.readFromVector(k, vector.child.asInstanceOf[T])
    }
    values.toVector
  }
}

class MapDeserializer[T <: ColumnVector, U <: ColumnVector](kdeser: OrcDeserializer[T],
                                                            vdeser: OrcDeserializer[U]) extends OrcDeserializer[MapColumnVector] {

  override def readFromVector(rowIndex: Int, vector: MapColumnVector): Map[Any, Any] = {

    // the row index is just a pointer into the list column vector offsets array to get the real pointer
    // the lengths will tell us how many to read from that point

    val offset = vector.offsets(rowIndex).toInt
    val length = vector.lengths(rowIndex).toInt

    val values = for (i <- offset until offset + length) yield {
      val key = kdeser.readFromVector(i, vector.keys.asInstanceOf[T])
      val value = vdeser.readFromVector(i, vector.values.asInstanceOf[U])
      key -> value
    }

    values.toMap
  }
}

class StructDeserializer(fields: Seq[Field], projectionColumns: Seq[Int]) extends OrcDeserializer[StructColumnVector] {
  require(fields.size == projectionColumns.size, "Struct should receive the fields that match the projection")

  private val deserializers = fields.map(_.dataType).map(OrcDeserializer.apply).toArray

  def readFromColumn[T <: ColumnVector](rowIndex: Int, k: Int, struct: StructColumnVector): Any = {
    val deser = deserializers(k).asInstanceOf[OrcDeserializer[T]]
    val colIndex = projectionColumns(k)
    val vector = struct.fields(colIndex).asInstanceOf[T]
    deser.readFromVector(if (vector.isRepeating) 0 else rowIndex, vector)
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
      try {
        val bytes = vector.vector.head.slice(vector.start(rowIndex), vector.start(rowIndex) + vector.length(rowIndex))
        new String(bytes, "UTF8")
      } catch {
        case e: Exception =>
          println(e)

          throw e
      }
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

object DateDeserializer extends OrcDeserializer[LongColumnVector] {
  val MILLIS_IN_DAY = 1000 * 60 * 60 * 24
  override def readFromVector(rowIndex: Int, vector: LongColumnVector): Any = {
    if (vector.isNull(rowIndex)) null
    else new java.util.Date(vector.vector(rowIndex) * MILLIS_IN_DAY)
  }
}

object BooleanDeserializer extends OrcDeserializer[LongColumnVector] {
  override def readFromVector(rowIndex: Int, vector: LongColumnVector): Boolean = vector.vector(rowIndex) == 1
}
