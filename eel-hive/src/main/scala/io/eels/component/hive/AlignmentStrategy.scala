package io.eels.component.hive

import io.eels.schema.StructType
import io.eels.{Rec, RowUtils}

trait AlignmentStrategy {
  def align(row: Rec, sourceSchema: StructType, targetSchema: StructType): Rec
}

/**
  * An AlignmentStrategy that will use default values, or nulls, to pad out rows
  * to match the target schema.
  */
object RowPaddingAlignmentStrategy extends AlignmentStrategy {
  override def align(row: Rec, sourceSchema: StructType, targetSchema: StructType): Rec = {
    val map = RowUtils.toMap(row, sourceSchema)
    // for each field in the metastore, get the field from the input row, and use that
    // if the input map does not have it, then pad it with a default or null
    targetSchema.fields.map { field =>
      map.getOrElse(field.name, field.default)
    }.toArray
  }
}