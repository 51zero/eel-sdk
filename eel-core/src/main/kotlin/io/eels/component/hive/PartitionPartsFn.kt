import io.eels.PartitionPart

// returns all the partition parts for a given row, if a row doesn't contain a value
// for a part then an error is thrown
object PartitionPartsFn extends Logging {
  def apply(row: InternalRow, partNames: Seq[String], schema: Schema): List[PartitionPart] = {
    require(partNames.forall(schema.columnNames.contains), "Schema must contain all partitions " + partNames)
    partNames.map { name =>
      val index = schema.indexOf(name)
      try {
        val value = row(index)
        require(!value.toString.contains(" "), s"Values for partitions cannot contain spaces $name=$value (index $index)")
        PartitionPart(name, value.toString)
      } catch {
        case NonFatal(e) =>
          logger.error(s"Could not get value for partition $name. Row=$row")
          throw e
      }
    }.toList
  }
}
