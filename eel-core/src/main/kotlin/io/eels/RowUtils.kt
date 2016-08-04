package io.eels

import io.eels.schema.Schema

object RowUtils {

  /**
   * Accepts a Row and reformats it according to the target schema, using the lookup map for the missing values.
   */
  fun rowAlign(row: Row, targetSchema: Schema, lookup: Map<String, Any> = emptyMap()): Row {
    val values = targetSchema.fieldNames().map { fieldName ->
      when {
        lookup.containsKey(fieldName) -> lookup[fieldName]
        else -> row.get(fieldName) ?: error("Missing value for field $fieldName")
      }
    }
    return Row(targetSchema, values)
  }
}