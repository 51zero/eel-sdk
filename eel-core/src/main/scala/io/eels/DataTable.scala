package io.eels

import io.eels.schema.StructType

// a fully inflated in memory representation of a data stream
case class DataTable(schema: StructType, records: Seq[Record]) {
  def map(f: Record => Record): DataTable = DataTable(schema, records.map(f))
  def filter(p: Record => Boolean): DataTable = DataTable(schema, records.filter(p))
}

case class Record(values: Rec)