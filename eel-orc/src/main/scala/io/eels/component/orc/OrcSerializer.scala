package io.eels.component.orc

import io.eels.coercion.{BigDecimalCoercer, SequenceCoercer, TimestampCoercer}
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
      case Category.BOOLEAN => LongColumnSerializer
      case Category.BYTE => LongColumnSerializer
      case Category.CHAR => BytesColumnSerializer
      case Category.DATE => LongColumnSerializer
      case Category.DECIMAL => DecimalSerializer
      case Category.DOUBLE => DoubleColumnSerializer
      case Category.FLOAT => DoubleColumnSerializer
      case Category.INT => LongColumnSerializer
      case Category.LONG => LongColumnSerializer
      case Category.SHORT => LongColumnSerializer
      case Category.STRING => BytesColumnSerializer
      case Category.STRUCT =>
        val serializers = desc.getChildren.asScala.map(forType)
        new StructSerializer(serializers)
      case Category.TIMESTAMP => TimestampColumnSerializer
      case Category.VARCHAR => BytesColumnSerializer
    }
  }
}

class StructSerializer(serializers: Seq[OrcSerializer[_]]) extends OrcSerializer[StructColumnVector] {

  def writeField[T <: ColumnVector](k: Int, vector: T, serializer: OrcSerializer[T], value: Any): Unit = {
    serializer.writeToVector(k, vector, value)
  }

  override def writeToVector(k: Int, vector: StructColumnVector, value: Any): Unit = {
    SequenceCoercer.coerce(value).zipWithIndex.foreach { case (value, index) =>
      val s = serializers(index).asInstanceOf[OrcSerializer[ColumnVector]]
      val subvector = vector.fields(index)
      writeField[ColumnVector](k, subvector, s, value)
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

object BytesColumnSerializer extends OrcSerializer[BytesColumnVector] {
  override def writeToVector(k: Int, vector: BytesColumnVector, value: Any): Unit = {
    if (value == null) {
      vector.isNull(k) = true
      vector.noNulls = false
    } else {
      val bytes = value.toString.getBytes("UTF8")
      vector.setRef(k, bytes, 0, bytes.length)
    }
  }
}

object LongColumnSerializer extends OrcSerializer[LongColumnVector] {
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
