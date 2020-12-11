package io.eels.component.orc

import io.eels.coercion._
import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.ql.exec.vector._
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable
import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category

import scala.collection.JavaConverters._

sealed trait OrcSerializer[T <: ColumnVector] {
  def writeToVector(k: Int, vector: T, value: Any)
}

object OrcSerializer {
  def forType(desc: TypeDescription): OrcSerializer[_ <: ColumnVector] = {
    desc.getCategory match {
      case Category.BINARY => BytesColumnSerializer
      case Category.BOOLEAN => BooleanSerializer
      case Category.BYTE => LongColumnSerializer
      case Category.CHAR => StringColumnSerializer
      case Category.DATE => LongColumnSerializer
      case Category.DECIMAL => DecimalSerializer
      case Category.DOUBLE => DoubleColumnSerializer
      case Category.FLOAT => DoubleColumnSerializer
      case Category.INT => LongColumnSerializer
      case Category.LIST => new ListSerializer(forType(desc.getChildren.get(0)))
      case Category.LONG => LongColumnSerializer
      case Category.MAP => new MapSerializer(forType(desc.getChildren.get(0)), forType(desc.getChildren.get(1)))
      case Category.SHORT => LongColumnSerializer
      case Category.STRING => StringColumnSerializer
      case Category.STRUCT =>
        val serializers = desc.getChildren.asScala.map(forType)
        new StructSerializer(serializers)
      case Category.TIMESTAMP => TimestampColumnSerializer
      case Category.VARCHAR => StringColumnSerializer
    }
  }
}

class MapSerializer[T <: ColumnVector, U <: ColumnVector](keySer: OrcSerializer[T],
                                                          valSer: OrcSerializer[U])
  extends OrcSerializer[MapColumnVector] {
  override def writeToVector(k: Int, vector: MapColumnVector, any: Any): Unit = {

    // the index k is used to index into the vector offsets

    val map = MapCoercer.coerce(any)

    if (k > 0)
      vector.offsets(k) = vector.offsets(k - 1) + vector.lengths(k - 1)
    vector.lengths(k) = map.size

    var i = vector.offsets(k).toInt

    map.foreach { case (key, value) =>
      keySer.writeToVector(i, vector.keys.asInstanceOf[T], key)
      valSer.writeToVector(i, vector.values.asInstanceOf[U], value)
      i = i + 1
    }
  }
}

class ListSerializer[T <: ColumnVector](nested: OrcSerializer[T]) extends OrcSerializer[ListColumnVector] {
  override def writeToVector(k: Int, vector: ListColumnVector, value: Any): Unit = {

    val xs = SequenceCoercer.coerce(value)

    if (k > 0)
      vector.offsets(k) = vector.offsets(k - 1) + vector.lengths(k - 1)
    vector.lengths(k) = xs.size

    var i = vector.offsets(k).toInt

    xs.foreach { x =>
      nested.writeToVector(i, vector.child.asInstanceOf[T], x)
      i = i + 1
    }
  }
}

class StructSerializer(serializers: Seq[OrcSerializer[_]]) extends OrcSerializer[StructColumnVector] {

  def writeField[T <: ColumnVector](k: Int, vector: T, serializer: OrcSerializer[T], value: Any): Unit = {
    serializer.writeToVector(k, vector, value)
  }

  override def writeToVector(k: Int, vector: StructColumnVector, value: Any): Unit = {
    SequenceCoercer.coerce(value).zipWithIndex.foreach { case (v, index) =>
      val s = serializers(index).asInstanceOf[OrcSerializer[ColumnVector]]
      val subvector = vector.fields(index)
      writeField[ColumnVector](k, subvector, s, v)
    }
  }
}

object TimestampColumnSerializer extends OrcSerializer[TimestampColumnVector] {
  override def writeToVector(k: Int, vector: TimestampColumnVector, value: Any): Unit = {
    if (value == null) {
      vector.setNullValue(k)
      vector.isNull(k) = true
      vector.noNulls = false
    } else {
      vector.set(k, TimestampCoercer.coerce(value))
    }
  }
}

object DecimalSerializer extends OrcSerializer[DecimalColumnVector] {
  override def writeToVector(k: Int, vector: DecimalColumnVector, value: Any): Unit = {
    if (value == null) {
      vector.isNull(k) = true
      vector.noNulls = false
    } else {
      val bd = BigDecimalCoercer.coerce(value).underlying()
      vector.vector(k) = new HiveDecimalWritable(HiveDecimal.create(bd))
    }
  }
}

object StringColumnSerializer extends OrcSerializer[BytesColumnVector] {
  override def writeToVector(k: Int, vector: BytesColumnVector, value: Any): Unit = {
    if (value == null) {
      vector.isNull(k) = true
      vector.noNulls = false
    } else {
      val bytes = StringCoercer.coerce(value).getBytes("UTF8")
      vector.setRef(k, bytes, 0, bytes.length)
    }
  }
}

object BytesColumnSerializer extends OrcSerializer[BytesColumnVector] {
  override def writeToVector(k: Int, vector: BytesColumnVector, value: Any): Unit = {
    if (value == null) {
      vector.isNull(k) = true
      vector.noNulls = false
    } else {
      val bytes = SequenceCoercer.coerce(value).asInstanceOf[Seq[Byte]].toArray
      vector.setRef(k, bytes, 0, bytes.length)
    }
  }
}

object BooleanSerializer extends OrcSerializer[LongColumnVector] {
  override def writeToVector(k: Int, vector: LongColumnVector, value: Any): Unit = {
    if (value == null) {
      vector.isNull(k) = true
      vector.noNulls = false
    } else {
      vector.vector(k) = if (BooleanCoercer.coerce(value)) 1 else 0
    }
  }
}

object LongColumnSerializer extends OrcSerializer[LongColumnVector] {
  val MILLIS_IN_DAY = 1000 * 60 * 60 * 24
  override def writeToVector(k: Int, vector: LongColumnVector, value: Any): Unit = {
    value match {
      case null =>
        vector.isNull(k) = true
        vector.noNulls = false
      case b:Boolean => vector.vector(k) = if (b) 1 else 0
      case l: Long => vector.vector(k) = l
      case i: Int => vector.vector(k) = i
      case s: Short => vector.vector(k) = s
      case b: Byte => vector.vector(k) = b
      case d: java.sql.Date => vector.vector(k) = d.getTime / MILLIS_IN_DAY
      case d: java.util.Date => vector.vector(k) = d.getTime / MILLIS_IN_DAY
    }
  }
}

object DoubleColumnSerializer extends OrcSerializer[DoubleColumnVector] {
  override def writeToVector(k: Int, vector: DoubleColumnVector, value: Any): Unit = {
    value match {
      case null =>
        vector.isNull(k) = true
        vector.noNulls = false
      case d: Double => vector.vector(k) = d
      case f: Float => vector.vector(k) = f
    }
  }
}