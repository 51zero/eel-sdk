package io.eels.component.hive.partition

import io.eels.schema.PartitionEntry
import org.apache.hadoop.fs.Path

case class PartitionMetaData(location: Path,
                             inputFormat: String,
                             outputFormat: String,
                             createTime: Long,
                             lastAccessTime: Long,
                             entries: Seq[PartitionEntry]) {

  // from key1=value1/key2=value2 will return Seq(value1,value2)
  def values(): Seq[String] = entries.map(_.value)

  // returns the PartitionEntry for the given key
  def get(key: String): Option[PartitionEntry] = entries.find(_.key == key)

  def value(key: String): Option[String] = get(key).map(_.value)
}
