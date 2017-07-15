package io.eels.component.hive.partition

import io.eels.component.hive.HiveOps
import io.eels.schema.Partition
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
  * A PartitionStrategy that requires all partitions to have been created in advance.
  */
object StaticPartitionStrategy extends PartitionStrategy {
  private val cache = scala.collection.mutable.Map.empty[Partition, Path]
  def ensurePartition(partition: Partition, dbName: String, tableName: String, client: IMetaStoreClient): Path = {
    cache.getOrElseUpdate(partition, {
      val ops = new HiveOps(client)
      ops.partitionMetaData(dbName, tableName, partition).location
    })
  }
}
