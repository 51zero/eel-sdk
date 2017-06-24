package io.eels.component.hive.partition

import io.eels.schema.Partition

// pluggable strategy that generates paths for partitions
trait PartitionPathStrategy {
  def name(partition: Partition): String
}

// generates hive paths using the default key1=value1/key2=value2 format
object DefaultHivePathStrategy extends PartitionPathStrategy {
  override def name(partition: Partition): String = partition.entries.map(_.unquoted).mkString("/")
}
