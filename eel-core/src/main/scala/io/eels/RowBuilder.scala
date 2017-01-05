package io.eels

import io.eels.schema.{Field, StructType}

class RowBuilder(schema: StructType) {

  private val map = new scala.collection.mutable.HashMap[Field, Any]()

  def put(index: Int, value: Any) = map.put(schema.field(index), value)

  def reset() = map.clear()

  def build(): Row = {
    val values = schema.fields.map { field =>
      map.getOrElse(field, field.default)
    }
    Row(schema, values)
  }
}
