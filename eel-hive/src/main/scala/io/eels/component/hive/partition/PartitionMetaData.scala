package io.eels.component.hive.partition

import io.eels.schema.{Partition, PartitionEntry}
import org.apache.hadoop.fs.Path

case class PartitionMetaData(location: Path,
                             // just the part of the path unique to the partition
                             // usually this will be the same as the entries flattened
                             name: String,
                             inputFormat: String,
                             outputFormat: String,
                             createTime: Long,
                             lastAccessTime: Long,
                             partition: Partition) {

  // from key1=value1/key2=value2 will return Seq(value1,value2)
  def values(): Seq[String] = partition.entries.map(_.value)

  // returns the PartitionEntry for the given key
  def get(key: String): Option[PartitionEntry] = partition.entries.find(_.key == key)

  def value(key: String): Option[String] = get(key).map(_.value)
}
