package io.eels

import io.eels.schema.StructType

// a fully inflated in memory representation of a data stream
case class DataTable(schema: StructType, values: Seq[Seq[Any]])