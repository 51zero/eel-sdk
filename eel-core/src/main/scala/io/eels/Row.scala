package io.eels

import com.sksamuel.exts.OptionImplicits._
import io.eels.schema.{Field, StringType, StructType}

object Row {
  val SentinelSingle = Row(StructType(Field("__sentinel__", StringType)), Array(null))
  val Sentinel = List(SentinelSingle)
  def apply(schema: StructType, values: Seq[Any]): Row = Row(schema, values.toIndexedSeq)
  def apply(schema: StructType, first: Any, rest: Any*): Row = new Row(schema, (first +: rest).toIndexedSeq)
  // this is needed so that apply(first,rest) doesn't override the apply from the case class
  def apply(schema: StructType, array: Array[Any]) = new Row(schema, array)

  def fromMap(schema: StructType, map: Map[String, Any]): Row = {
    Row(schema, schema.fieldNames.map(map.apply))
  }
}

case class Row(schema: StructType, values: IndexedSeq[Any]) {

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
  def apply(name: String): Any = get(name)

  def get(k: Int): Any = values(k)
  def getOpt(name: String, caseSensitive: Boolean = true): Option[Any] = {
    val index = schema.indexOf(name, caseSensitive)
    if (index < 0) None else values(index).some
  }

  def getAs[T](name: String, caseSensitive: Boolean = true): T = get(name, caseSensitive).asInstanceOf[T]
  def get(name: String, caseSensitive: Boolean = true): Any = {
    getOpt(name, caseSensitive).getOrError(s"$name did not exist in row")
  }

  def size(): Int = values.size

  def replace(name: String, value: Any, caseSensitive: Boolean = true): Row = {
    val k = schema.indexOf(name, caseSensitive)
    // todo this could be optimized to avoid the copy
    val newValues = values.updated(k, value)
    copy(values = newValues)
  }

  def map(name: String, fn: Any => Any, caseSensitive: Boolean = true): Row = {
    val k = schema.indexOf(name, caseSensitive)
    val value = get(k)
    val newValues = values.updated(k, value)
    copy(values = newValues)
  }

  def mapIfExists(name: String, fn: Any => Any, caseSensitive: Boolean = true): Row = {
    val k = schema.indexOf(name, caseSensitive)
    if (k > 0) {
      val value = get(k)
      val newValues = values.updated(k, value)
      copy(values = newValues)
    } else {
      this
    }
  }

  def containsValue(value: Any): Boolean = values.contains(value)

  def add(name: String, value: Any): Row = copy(schema = schema.addField(name), values = values :+ value)
}