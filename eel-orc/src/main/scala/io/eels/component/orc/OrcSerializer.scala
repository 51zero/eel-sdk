package io.eels.component.orc

import io.eels.coercion.BigDecimalCoercer
import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.ql.exec.vector._
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable
import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category

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
      case Category.TIMESTAMP => LongColumnSerializer
      case Category.VARCHAR => BytesColumnSerializer
    }
  }
}

object DecimalSerializer extends OrcSerializer[DecimalColumnVector] {
  override def writeToVector(k: Int, vector: DecimalColumnVector, value: Any): Unit = {
    val bd = BigDecimalCoercer.coerce(value).underlying()
    vector.vector(k) = new HiveDecimalWritable(HiveDecimal.create(bd))
  }
}

object BytesColumnSerializer extends OrcSerializer[BytesColumnVector] {
  override def writeToVector(k: Int, vector: BytesColumnVector, value: Any): Unit = {
    val bytes = if (value == null) Array.emptyByteArray else value.toString.getBytes("UTF8")
    vector.setRef(k, bytes, 0, bytes.length)
  }
}

object LongColumnSerializer extends OrcSerializer[LongColumnVector] {
  override def writeToVector(k: Int, vector: LongColumnVector, value: Any): Unit = {
    value match {
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
      case d: Double => vector.vector(k) = d
      case f: Float => vector.vector(k) = f
    }
  }
}
