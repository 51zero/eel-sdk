package io.eels.component.hive

import com.sksamuel.exts.Logging
import io.eels.{PartitionPart, Row}

object PartitionPartsFn extends Logging {

  /**
   * For a given row, will return the list of PartitionPart's that match the given list of part names.
   */
  def rowPartitionParts(row: Row, partNames: List[String]): List[PartitionPart] = {
    require(partNames.forall { name => row.schema.fieldNames().contains(name) }, s"Schema must contain all partitions $partNames")

    partNames.map { it =>
      val index = row.schema.indexOf(it)
      try {
        val value = row.values(index)
        require(!value.toString().contains(" "), s"Values for partitions cannot contain spaces $it=$value (index $index)")
        PartitionPart(it, value.toString())
      } catch {
        case t: Throwable =>
          logger.error(s"Could not get value for partition $it. Row=$row")
          throw t
      }
    }
  }
}
