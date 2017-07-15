package io.eels.component.hive.partition

import io.eels.schema.Partition
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
  * Strategy that is invoked for each partition. The strategy determines how the metastore is updated
  * to handle partitions. For example, it may choose to create partitions that don't exist, or throw
  * an exception if the partition does not already exist.
  *
  * The strategy also determines the location of the path where the partition is located,
  * so custom implemntations can place partitions in non standard locations if required.
  *
  * Implementations do not need to be thread safe. Each strategy will only be called by a single
  * thread at a time.
  */
trait PartitionStrategy {
  // for the given partition, the strategy must return the full path for the partition or throw an exception
  def ensurePartition(partition: Partition, dbName: String, tableName: String, client: IMetaStoreClient): Path
}



