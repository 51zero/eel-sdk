package io.eels.component.hive

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.{Table, Partition => HivePartition}

import scala.collection.JavaConverters._

// represents a full partition, which, against what you might think, in hive speak is the full
// set of partition key values, eg key1=value1/key2=value2/key3=value3, ie, key=value is not a partition,
case class Partition(parts: List[PartitionPart]) {

  // returns the partition in normalized directory representation, eg key1=value1/key2=value2/...
  // hive seems to call this the partition name, at least client.listPartitionNames returns these
  def name: String = parts.map(_.unquoted).mkString("/")

  // from key1=value1/key2=value2 will return key1,key2
  def keys: List[String] = parts.map(_.key)

  // from key1=value1/key2=value2 will return value1,value2
  def values: List[String] = parts.map(_.value)

  // returns the partition value for the given key
  def apply(key: String): String = get(key).get

  def get(key: String): Option[String] = parts.find(_.key == key).map(_.value)
}

object Partition {

  def fromPartition(table: Table, partition: HivePartition): Partition = {
    apply(table.getPartitionKeys.asScala.map(_.getName), partition.getValues.asScala)
  }

  // creates a partition from the list of keys and the list of values
  def apply(keys: Seq[String], values: Seq[String]): Partition = {
    require(
      keys.size == values.size,
      s"A Partition must have the same number of contents in both the keys and the values collections. Keys=$keys but values=$values"
    )
    val parts = keys.zip(values).map { case (key, value) => PartitionPart(key, value) }
    Partition(parts.toList)
  }

  // parses a partition path
  def apply(name: String): Partition = {
    Partition(
      name.split('/').map(_.split("=") match {
        case Array(key, value) => PartitionPart(key, value)
      }).toList
    )
  }

  val Empty = Partition(Nil)
}

// represents part of a partition, eg a single key=value pair, which is what people might think a partition
// is normally. For example, name=sam is not a partition in hive speak (it doesn't seem to have a name in hive)
// so I've decided to call it PartitionPart
case class PartitionPart(key: String, value: String) {
  // returns the key value part in the standard hive key=value format
  def unquoted: String = s"$key=$value"
}

object PartitionPart {
  def unapply(path: Path): Option[(String, String)] = unapply(path.getName)
  def unapply(str: String): Option[(String, String)] = str.split('=') match {
    case Array(a, b) => Some((a, b))
    case _ => None
  }
}

// represents a single partition key and all its values
case class PartitionKey(name: String, values: Seq[String])

