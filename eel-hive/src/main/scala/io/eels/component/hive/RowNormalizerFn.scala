package io.eels.component.hive

import io.eels.Row
import io.eels.schema.StructType

/**
  * Will align a row with the given hive file schema. This means it will strip out of the row any fields/values
  * which are not in the given file schena,
  * If eel.hive.sink.pad-with-null is true, it will pad any fields missing with null.
  */
class RowNormalizerFn(fileSchema: StructType) {

  private val config = HiveSinkConfig()

  //  // these are the indexes of the cells to skip in each row because they are partition values
  //  // we build up the ones to skip, then we can make an array that contains only the ones to keep
  //  // this array is used to iterate over using the indexes to pick out the values from the row quickly
  //  val indexesToSkip = if (config.includePartitionsInData) Nil else partitionKeyNames.map(sourceSchema.indexOf)
  //  val indexesToWrite = List.range(0, sourceSchema.size).filterNot(indexesToSkip.contains)
  //  assert(indexesToWrite.nonEmpty, "Cannot write frame where all fields are partitioned")

  private def isIncludedField(fieldName: String) = fileSchema.fieldNames.contains(fieldName)

  private def default(fieldName: String) = {
    if (config.padNulls) null
    else sys.error(s"Missing value in row for field=$fieldName; if you wish to use nulls for missing values then set 'eel.hive.sink.pad-with-null=true'")
  }

  def apply(row: Row): Row = {
    val map = row.schema.fieldNames().zip(row.values).toMap
    val values = fileSchema.fieldNames.filter(isIncludedField).map { it =>
      // if the map doesn't contain a value for the field we'll try getting a default
      map.getOrElse(it, default(it))
    }
    Row(fileSchema, values)
  }
}
