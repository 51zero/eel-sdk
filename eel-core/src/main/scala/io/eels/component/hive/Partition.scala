package io.eels.component.hive

import org.apache.hadoop.fs.Path

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
}

object Partition {
  def apply(name: String): Partition = {
    Partition(
      name.split('/').map(_.split("=") match {
        case Array(key, value) => PartitionPart(key, value)
      }).toList
    )
  }
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