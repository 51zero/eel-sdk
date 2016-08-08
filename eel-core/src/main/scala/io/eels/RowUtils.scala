package io.eels

import io.eels.schema.Schema

object RowUtils {

  /**
   * Accepts a Row and reformats it according to the target schema, using the lookup map for the missing values.
   */
  def rowAlign(row: Row, targetSchema: Schema, lookup: Map[String, Any] = Map.empty): Row = {
    val values = targetSchema.fieldNames().map { name =>
      if (lookup.contains(name)) lookup(name) else row.get(name)
    }
    Row(targetSchema, values.toVector)
  }
}