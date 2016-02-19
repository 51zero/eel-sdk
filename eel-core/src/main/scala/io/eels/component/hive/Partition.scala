package io.eels.component.hive

// represents a full partition, which in hive terminlogy is (key1=value1, key2=value2, ...)
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

// represents part of a partition, eg a single key value pair
case class PartitionKeyValue(key: String, value: String) {
  def unquoted: String = s"$key=$value"
}

