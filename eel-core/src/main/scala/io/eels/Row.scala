package io.eels

import io.eels.schema.{Field, Schema}

object Row {
  val PoisonPill = Row(Schema(Field("a")), Vector[Any](null))
  def apply(schema: Schema, first: Any, rest: Any*): Row = apply(schema, (first +: rest).toVector)
}

case class Row(schema: Schema, values: Vector[Any]) {
  require(
    schema.size() == values.size,
    s"Row should have a value for each field (${schema.fields.size} fields=${schema.fieldNames().mkString(",")}, ${values.size} values=${values.mkString(",")})"
  )

  override def toString: String = {
    schema.fieldNames().zip(values).map { case (name, value) =>
      s"$name = ${if (value == null) " NULL" else value}"
    }.mkString("[", ",", "]")
  }

  def apply(k: Int): Any = get(k)
  def get(k: Int): Any = values(k)

  def get(name: String, caseInsensitive: Boolean = false): Any = {
    val index = schema.indexOf(name, caseInsensitive)
    if (index < 0)
      sys.error(s"$name did not exist in row")
    values(index)
  }

  def size(): Int = values.size

  def replace(name: String, value: Any, caseSensitive: Boolean): Row = {
    val k = schema.indexOf(name, caseSensitive)
    // todo this could be optimized to avoid the copy
    val newValues = values.updated(k, value)
    copy(values = newValues)
  }

  def add(name: String, value: Any): Row = copy(schema = schema.addField(name), values = values :+ value)
}