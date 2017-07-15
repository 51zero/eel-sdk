package io.eels.component.hive

import io.eels.Row
import io.eels.schema.StructType

trait AlignmentStrategy {
  def align(row: Row, targetSchema: StructType): Row
}

/**
  * An AlignmentStrategy that will use default values, or nulls, to pad out rows
  * to match the target schema.
  */
object RowPaddingAlignmentStrategy extends AlignmentStrategy {
  override def align(row: Row, targetSchema: StructType): Row = {
    val map = row.schema.fieldNames().zip(row.values).toMap
    // for each field in the metastore, get the field from the input row, and use that
    // if the input map does not have it, then pad it with a default or null
    val values = targetSchema.fields.map { field =>
      map.getOrElse(field.name, field.default)
    }
    Row(targetSchema, values)
  }
}