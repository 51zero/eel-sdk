package io.eels.component.hive.partition

import com.sksamuel.exts.Logging
import io.eels.Row
import io.eels.schema.{Partition, PartitionEntry}

import scala.util.control.NonFatal

object RowPartitionFn extends Logging {

  /**
    * Will return a Partition created from the values of a row, based on the schema fields.
    */
  def apply(row: Row): Partition = apply(row, row.schema.partitions.map(_.name))

  /**
    * Will return a Partition created from the values of a row, based on the given list of partition keys.
    */
  def apply(row: Row, partitionKeys: Seq[String]): Partition = {
    require(
      partitionKeys.forall { key => row.schema.fieldNames().contains(key) },
      s"The row schema must include data for all partitions; schema fields=${row.schema.fieldNames()}; expected partitions=$partitionKeys"
    )

    val entries = partitionKeys.map { fieldName =>
      val index = row.schema.indexOf(fieldName)
      try {
        val value = row.values(index)
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
