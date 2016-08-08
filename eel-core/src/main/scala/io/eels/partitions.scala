package io.eels

import io.eels.schema.Field
import org.apache.hadoop.hive.metastore.api.StorageDescriptor

// Represents a partition, which is the full set of partition key/values pairs for a particular record
// eg key1=value1/key2=value2/key3=value3 is a partition

@deprecated("use partition actual")
case class PartitionSpec(parts: Array[PartitionPart]) {

  // returns the partition in normalized directory representation, eg key1=value1/key2=value2/...
  // hive seems to call this the partition name, at least client.listPartitionNames returns these
  def name(): String = parts.map(_.unquoted).mkString("/")

  // from key1=value1/key2=value2 will return key1,key2
  def keys(): Array[String] = parts.map(_.key)

  // from key1=value1/key2=value2 will return List(value1,value2)
  def values(): Array[String] = parts.map(_.value)

  // returns the partition value for the given key
  def get(key: String): String = parts.find(_.key == key).get.value
}

object PartitionSpec {
  val empty = PartitionSpec(Array.empty)
  def parsePath(path: String): PartitionSpec = {
    val parts = path.split("/").map { part =>
      val parts = part.split("=")
      PartitionPart(parts.head, parts.last)
    }
    PartitionSpec(parts)
  }
}

case class PartitionKey(field: Field,
                        createTime: Long,
                        parameters: Map[String, String])

// Represents a partition, which is the full set of partition key/values pairs for a particular record
// eg key1=value1/key2=value2/key3=value3 is a partition
case class Partition(createTime: Long,
                     sd: StorageDescriptor,
                     values: List[PartitionPart]) {
  // returns the partition in normalized directory representation, eg key1=value1/key2=value2/...
  // hive calls this the partition name, eg client.listPartitionNames returns these
  def name(): String = values.map(_.unquoted()).mkString("/")
}

object Partition {
  def fromName(name: String): Partition = {
    val parts = name.split('/').map { x =>
      val xs = x.split('=')
      PartitionPart(xs.head, xs.last)
    }
    Partition(0, new StorageDescriptor(), parts.toList)
  }
}

// a single "part" in a partition, ie in country=usa/state=alabama, a value would be state=alabama
case class PartitionPart(key: String, value: String) {

  // returns the key value part in the standard hive key=value format with unquoted values
  def unquoted(): String = s"$key=$value"

  // returns the key value part in the standard hive key=value format with quoted values
  def quoted(): String = s"$key='$value'"
}