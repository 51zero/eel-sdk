package io.eels.component.csv

import io.eels.schema.StructType

object CsvPredef {
  type SkipRowCallback = (Int, StructType, Array[String]) => Boolean
}