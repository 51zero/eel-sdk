package io.eels.component.hive

import io.eels.{Schema, _}
import org.apache.hadoop.fs.Path

// represents a full partition, which, against what you might think, in hive speak is the full
// path of partitions, eg key1=value1/key2=value2/key3=value3
case class Partition(kvs: List[PartitionKeyValue]) {
  // returns the partition in directory representation, eg key1=value1/key2=value2/...
  def dirName: String = kvs.map(_.unquoted).mkString("/")
  def keys: List[String] = kvs.map(_.key)
  def values: List[String] = kvs.map(_.value)
}

object Partition {
  def apply(name: String): Partition = {
    Partition(
      name.split('/').map(_.split("=") match {
        case Array(key, value) => PartitionKeyValue(key, value)
      }).toList
    )
  }
}

// represents part of a partition, eg a single key value pair, which is what people might think a partition
// is normally. For example, name=sam is not a partition in hive speak, but a partition value.
case class PartitionKeyValue(key: String, value: String) {
  def unquoted: String = s"$key=$value"
}

@deprecated("will be replaced with PartitionKeyValue", "0.24.0")
case class PartitionPart(key: String, value: String) {
  def unquotedDir = s"$key=$value"
}

@deprecated("will be replaced with PartitionKeyValue", "0.24.0")
object PartitionPart {
  def unapply(path: Path): Option[(String, String)] = unapply(path.getName)
  def unapply(str: String): Option[(String, String)] = str.split('=') match {
    case Array(a, b) => Some((a, b))
    case _ => None
  }
}

// returns all the partition parts for a given row, if a row doesn't contain a value
// for a part then an error is thrown
object RowPartitionParts {
  def apply(row: InternalRow, partNames: Seq[String], schema: Schema): List[PartitionPart] = {
    require(partNames.forall(schema.columnNames.contains), "FrameSchema must contain all partitions " + partNames)
    partNames.map { name =>
      val index = schema.indexOf(name)
      val value = row(index)
      require(!value.toString.contains(" "), "Values for partition fields cannot contain spaces")
      PartitionPart(name, value.toString)
    }.toList
  }
}