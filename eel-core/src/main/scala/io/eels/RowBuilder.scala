package io.eels

import io.eels.schema.StructType

class RowBuilder(schema: StructType) {

  private var values: Array[Any] = _
  reset()

  def put(index: Int, value: Any) = {
    // parquet can send us the same index twice, for a repeated element, must convert to a collection
    if (values(index) == null) {
      values(index) = value
    } else {
      val current = values(index)
      current match {
        case vector: Vector[_] => values(index) = vector :+ value
        case single => values(index) = Vector(current, value)
      }
    }
  }

  def reset() = {
    values = Array.ofDim[Any](schema.size)
  }

  def build(): Row = Row(schema, values)
}
