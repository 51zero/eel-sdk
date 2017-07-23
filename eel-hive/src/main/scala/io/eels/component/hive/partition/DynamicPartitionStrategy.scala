package io.eels.component.hive.partition

import io.eels.component.hive.HiveOps
import io.eels.schema.Partition
import io.eels.util.HdfsMkdir
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
  * A PartitionStrategy that creates partitions in the table location using the default
  * key=value/key=value path format
  */
class DynamicPartitionStrategy extends PartitionStrategy {

  private val cache = scala.collection.mutable.Map.empty[Partition, Path]

  def ensurePartition(partition: Partition,
                      dbName: String,
                      tableName: String,
                      inheritPermissions: Boolean,
                      client: IMetaStoreClient)(implicit fs: FileSystem): Path = {

    def createPartition: Path = this.synchronized {
      val ops = new HiveOps(client)
      ops.partitionMetaData(dbName, tableName, partition) match {
        case Some(meta) => meta.location
        case _ =>
          val tableLocation = ops.tablePath(dbName, tableName)
          val partitionPath = new Path(tableLocation, partition.unquoted)
          ops.createPartitionIfNotExists(dbName, tableName, partition, partitionPath)
          HdfsMkdir(partitionPath, inheritPermissions)
          partitionPath
      }
    }

    cache.getOrElseUpdate(partition, createPartition)
  }
}
