package io.eels

import io.eels.schema.StructType

object RowUtils {

  /**
    * Accepts a Row and reformats it according to the target schema, using the lookup map
    * for missing or replacement values.
   */
  def rowAlign(row: Row, targetSchema: StructType, lookup: Map[String, Any] = Map.empty): Row = {
    val values = targetSchema.fieldNames().map { name =>
      if (lookup.contains(name)) lookup(name) else row.get(name)
    }
    Row(targetSchema, values.toVector)
  }
}