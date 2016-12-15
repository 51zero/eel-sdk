package io.eels

import io.eels.schema.StructType

import scala.collection.immutable.Vector

class RowBuilder(schema: StructType) {
  private var values = Vector.newBuilder[Any]
  def add(value: Any) = values.+=(value)
  def reset() = values = Vector.newBuilder[Any]
  def build(): Row = Row(schema, values.result)
}
