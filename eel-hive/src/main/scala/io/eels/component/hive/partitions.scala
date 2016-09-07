package io.eels.component.hive

import io.eels.schema.{Field, PartitionPart}
import org.apache.hadoop.hive.metastore.api.StorageDescriptor

// Represents a partition, which is the full set of partition key/values pairs for a particular record
// eg key1=value1/key2=value2/key3=value3 is a partition

case class PartitionKey(field: Field)

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

