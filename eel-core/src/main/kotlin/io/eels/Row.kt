package io.eels

import io.eels.schema.Field
import io.eels.schema.Schema

data class Row(val schema: Schema, val values: List<Any?>) {

  constructor(schema: Schema, vararg values: Any?) : this(schema, values.toList())

  init {
    require(
        schema.size() == values.size,
        {
          "Row should have a value for each field (${schema.fields.size} fields=${schema.fieldNames().joinToString(",")}, ${values.size} values=${values.joinToString(",")})"
        }
    )
  }

  override fun toString(): String {
    return schema.fieldNames().zip(values).map {
      "${it.first} = ${if (it.second == null) "NULL" else it.second.toString()}"
    }.joinToString(",", "[", "]")
  }

  fun get(k: Int): Any? = values[k]

  fun get(name: String, caseInsensitive: Boolean = false): Any? {
    val index = schema.indexOf(name, caseInsensitive)
    return values[index]
  }

  companion object {
    val PoisonPill = Row(Schema(Field("a")), listOf(object : Any() {}))
  }

  fun size(): Int = values.size

  fun replace(name: String, value: Any, caseSensitive: Boolean): Row {
    val k = schema.indexOf(name, caseSensitive)
    // todo this could be optimized to avoid the copy
    val newValues = values.toMutableList()
    newValues[k] = value
    return copy(values = newValues.toList())
  }

  fun add(name: String, value: Any): Row = copy(schema = schema.addField(name), values = values + value)
}