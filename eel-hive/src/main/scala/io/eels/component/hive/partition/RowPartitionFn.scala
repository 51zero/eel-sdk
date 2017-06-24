package io.eels.component.hive.partition

import com.sksamuel.exts.Logging
import io.eels.Row
import io.eels.schema.{Partition, PartitionEntry}

import scala.util.control.NonFatal

object RowPartitionFn extends Logging {

  /**
    * Will return a Partition created from the values of a row, based on the schema.
    */
  def apply(row: Row): Partition = apply(row, row.schema.partitions.map(_.name))

  /**
    * Will return a Partition created from the values of a row, based on the given list of partition keys.
    */
  def apply(row: Row, partitionKeys: Seq[String]): Partition = {
    require(
      partitionKeys.forall { key => row.schema.fieldNames().contains(key) },
      s"The schema must include data for all partitions; otherwise the writer wouldn't be able to create the correct partition path; schema fields=${row.schema.fieldNames()}; expected partitions=$partitionKeys"
    )

    val entries = partitionKeys.map { it =>
      val index = row.schema.indexOf(it)
      try {
        val value = row.values(index)
        require(!value.toString().contains(" "), s"Values for partitions cannot contain spaces $it=$value (index $index)")
        PartitionEntry(it, value.toString)
      } catch {
        case NonFatal(t) =>
          logger.error(s"Could not get value for partition $it. Row=$row")
          throw t
      }
    }

    Partition(entries)
  }
}
