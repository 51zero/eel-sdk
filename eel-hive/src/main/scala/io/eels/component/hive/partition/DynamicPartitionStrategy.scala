package io.eels.component.hive.partition

import io.eels.component.hive.HiveOps
import io.eels.schema.Partition
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
  * A PartitionStrategy that creates partitions in the table location using the default
  * key=value/key=value path format
  */
object DynamicPartitionStrategy extends PartitionStrategy {
  private val cache = scala.collection.mutable.Map.empty[Partition, Path]
  def ensurePartition(partition: Partition, dbName: String, tableName: String, client: IMetaStoreClient): Path = {
    cache.getOrElseUpdate(partition, {
      val ops = new HiveOps(client)
      val tableLocation = ops.tablePath(dbName, tableName)
      val partitionPath = new Path(tableLocation, partition.unquoted)
      ops.createPartitionIfNotExists(dbName, tableName, partition, partitionPath)
      partitionPath
    })
  }
}
