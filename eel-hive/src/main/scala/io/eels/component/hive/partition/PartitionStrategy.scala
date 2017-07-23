package io.eels.component.hive.partition

import io.eels.schema.Partition
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
  * Strategy that is invoked for each partition. The strategy determines how the metastore is updated
  * to handle partitions. For example, it may choose to create partitions that don't exist, or throw
  * an exception if the partition does not already exist.
  *
  * Note: Any new paths must be created on disk by this strategy.
  *
  * The strategy also determines the location of the path where the partition is located,
  * so custom implementations can place partitions in non standard locations if required.
  *
  * Note: Implementations must be thread safe as multiple writers may invoke
  * the strategy concurrently for the same partition value.
  */
trait PartitionStrategy {
  // for the given partition, the strategy must return the full path for the partition or throw an exception
  def ensurePartition(partition: Partition,
                      dbName: String,
                      tableName: String,
                      inheritPermissions: Boolean,
                      client: IMetaStoreClient)(implicit fs: FileSystem): Path
}



