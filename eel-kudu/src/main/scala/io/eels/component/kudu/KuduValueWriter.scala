package io.eels.component.kudu

import java.nio.ByteBuffer

import io.eels.coercion.{DoubleCoercer, FloatCoercer, IntCoercer, LongCoercer}
import org.apache.kudu.client.PartialRow

trait KuduValueWriter {
  def write(row: PartialRow, index: Int, value: Any): Unit
}

object KuduStringWriter extends KuduValueWriter {
  override def write(row: PartialRow, index: Int, value: Any): Unit = {
    row.addString(index, value.toString)
  }
}

object KuduBinaryWriter extends KuduValueWriter {
  override def write(row: PartialRow, index: Int, value: Any): Unit = {
    value match {
      case array: Array[Byte] => row.addBinary(index, array)
      case buffer: ByteBuffer => row.addBinary(index, buffer)
    }
  }
}

object KuduLongWriter extends KuduValueWriter {
  override def write(row: PartialRow, index: Int, value: Any): Unit = {
    row.addLong(index, LongCoercer.coerce(value))
  }
}

object KuduIntWriter extends KuduValueWriter {
  override def write(row: PartialRow, index: Int, value: Any): Unit = {
    row.addInt(index, IntCoercer.coerce(value))
  }
}

object KuduBooleanWriter extends KuduValueWriter {
  override def write(row: PartialRow, index: Int, value: Any): Unit = {
    row.addBoolean(index, value.asInstanceOf[Boolean])
  }
}

object KuduByteWriter extends KuduValueWriter {
  override def write(row: PartialRow, index: Int, value: Any): Unit = {
    row.addByte(index, value.asInstanceOf[Byte])
  }
}

object KuduDoubleWriter extends KuduValueWriter {
  override def write(row: PartialRow, index: Int, value: Any): Unit = {
    row.addDouble(index, DoubleCoercer.coerce(value))
  }
}

object KuduFloatWriter extends KuduValueWriter {
  override def write(row: PartialRow, index: Int, value: Any): Unit = {
    row.addFloat(index, FloatCoercer.coerce(value))
  }
}

object KuduShortWriter extends KuduValueWriter {
  override def write(row: PartialRow, index: Int, value: Any): Unit = {
    row.addShort(index, value.asInstanceOf[Short])
  }
}