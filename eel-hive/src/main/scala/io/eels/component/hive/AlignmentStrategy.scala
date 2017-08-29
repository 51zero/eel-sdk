package io.eels.component.hive

import io.eels.Row
import io.eels.schema.StructType

/**
  * An alignment strategy will accept an input Row and return an output Row that is compatible
  * with the target schema. This allows writing to sinks whereby the output schema is not the
  * same as the input schema.
  *
  * For example, the input may come from a JDBC table, and an output Hive table only defines
  * a subset of the columns. Each row would need to be aligned so that it matches the subset schema.
  *
  * Implementations are free to add values, drop values or throw an exception if they wish.
  */
trait AlignmentStrategy {
  def create(targetSchema: StructType): RowAligner
}

trait RowAligner {
  def align(row: Row): Row
}

/**
  * An AlignmentStrategy that will use default values, or nulls, to pad out rows
  * to match the target schema, dropping any fields that exist in the input, but not the output, schema
  */
object RowPaddingAlignmentStrategy extends AlignmentStrategy {

  override def create(targetSchema: StructType): RowAligner = new RowAligner {
    val fields = targetSchema.fields
    override def align(row: Row): Row = {
      val values = for (field <- fields) yield {
        row.getOpt(field.name).getOrElse(field.default)
      }
      Row(targetSchema, values)
    }
  }
}