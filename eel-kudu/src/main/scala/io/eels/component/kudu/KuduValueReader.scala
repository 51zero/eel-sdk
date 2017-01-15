package io.eels.component.kudu

import org.apache.kudu.client.RowResult

trait KuduValueReader[V] {
  def read(row: RowResult, index: Int): V
}

object StringValueReader extends KuduValueReader[String] {
  override def read(row: RowResult, index: Int): String = row.getString(index)
}

object BinaryValueReader extends KuduValueReader[Array[Byte]] {
  override def read(row: RowResult, index: Int): Array[Byte] = row.getBinaryCopy(index)
}

object BooleanValueReader extends KuduValueReader[Boolean] {
  override def read(row: RowResult, index: Int): Boolean = row.getBoolean(index)
}

object ByteValueReader extends KuduValueReader[Byte] {
  override def read(row: RowResult, index: Int): Byte = row.getByte(index)
}

object DoubleValueReader extends KuduValueReader[Double] {
  override def read(row: RowResult, index: Int): Double = row.getDouble(index)
}

object FloatValueReader extends KuduValueReader[Float] {
  override def read(row: RowResult, index: Int): Float = row.getFloat(index)
}

object IntValueReader extends KuduValueReader[Int] {
  override def read(row: RowResult, index: Int): Int = row.getInt(index)
}

object LongValueReader extends KuduValueReader[Long] {
  override def read(row: RowResult, index: Int): Long = row.getLong(index)
}

object ShortValueReader extends KuduValueReader[Short] {
  override def read(row: RowResult, index: Int): Short = row.getShort(index)
}