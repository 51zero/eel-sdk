package io.eels.component.hive.partition

import com.sksamuel.exts.Logging
import io.eels.Rec
import io.eels.schema.{Partition, PartitionEntry, StructType}

import scala.util.control.NonFatal

object RowPartitionFn extends Logging {

  /**
    * Will return a Partition created from the values of a row, based on the given list of partition keys.
    */
  def apply(row: Rec, schema: StructType, partitionKeys: Seq[String]): Partition = {
    require(
      partitionKeys.forall { key => schema.fieldNames().contains(key) },
      s"The schema must include data for all partitions; otherwise the writer wouldn't be able to create the correct partition path; schema fields=${schema.fieldNames()}; expected partitions=$partitionKeys"
    )

    val entries = partitionKeys.map { fieldName =>
      val index = schema.indexOf(fieldName)
      try {
        val value = row(index)
        require(value != null, s"Partition value cannot be null for $fieldName")
        require(value.toString.trim.nonEmpty, s"Partition value cannot be empty for $fieldName")
        require(!value.toString.contains(" "), s"Values for partitions cannot contain spaces $fieldName=$value (index $index)")
        PartitionEntry(fieldName, value.toString)
      } catch {
        case NonFatal(t) =>
          logger.error(s"Could not get value for partition $fieldName. Row=$row")
          throw t
      }
    }

    Partition(entries)
  }
}
