package io.eels

import io.eels.schema.{Field, StringType, StructType}

object Row {
  val Sentinel = new Row(StructType(Field("a", StringType)), Array(null))
  def apply(schema: StructType, first: Any, rest: Any*): Row = new Row(schema, first +: rest)
  def apply(schema: StructType, array: Array[Any]) = new Row(schema, array)
}

case class Row(schema: StructType, values: Seq[Any]) {

  require(
    schema.size == values.size,
    s"Row should have a value for each field (${schema.fields.size} fields=${schema.fieldNames().mkString(",")}, ${values.size} values=${values.mkString(",")})"
  )

  override def toString: String = {
    schema.fieldNames().zip(values).map { case (name, value) =>
      s"$name = ${if (value == null) " NULL" else value}"
    }.mkString("[", ",", "]")
  }

  // returns the data in this row as a Map
  def map(): Map[String, Any] = {
    values.zip(schema.fieldNames).map { case (value, name) =>
      name -> value
    }.toMap
  }

  def replaceSchema(newSchema: StructType): Row = Row(newSchema, values)

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