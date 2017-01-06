package io.eels

import io.eels.schema.StructType

class RowBuilder(schema: StructType) {

  private var values: Array[Any] = _
  reset()

  def put(index: Int, value: Any) = values(index) = value

  def reset() = values = Array.ofDim[Any](schema.size)

  def build(): Row = Row(schema, values)
}
