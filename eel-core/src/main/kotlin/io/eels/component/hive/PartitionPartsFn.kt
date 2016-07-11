import io.eels.Row
import io.eels.component.hive.PartitionPart
import io.eels.util.Logging

object PartitionPartsFn : Logging {

  /**
   * For a given row, will return the list of PartitionPart's that match the given list of part names.
   */
  fun rowPartitionParts(row: Row, partNames: List<String>): List<PartitionPart> {
    require(partNames.all { row.schema.fieldNames().contains(it) }, { "Schema must contain all partitions $partNames" })

    return partNames.map {
      val index = row.schema.indexOf(it)
      try {
        val value = row.values[index]
        require(!value.toString().contains(" "), { "Values for partitions cannot contain spaces $it=$value (index $index)" })
        PartitionPart(it, value.toString())
      } catch(e: Exception) {
        logger.error("Could not get value for partition $it. Row=$row")
        throw e
      }
    }
  }
}
