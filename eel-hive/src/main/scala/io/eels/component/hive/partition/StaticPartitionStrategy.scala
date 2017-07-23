package io.eels.component.hive.partition

import io.eels.component.hive.HiveOps
import io.eels.schema.Partition
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import com.sksamuel.exts.OptionImplicits._

/**
  * A PartitionStrategy that requires all partitions to have been created in advance.
  */
object StaticPartitionStrategy extends PartitionStrategy {
  private val cache = scala.collection.mutable.Map.empty[Partition, Path]
  def ensurePartition(partition: Partition, dbName: String, tableName: String, inheritPermissions: Boolean, client: IMetaStoreClient)(implicit fs: FileSystem): Path = {
    cache.getOrElseUpdate(partition, {
      val ops = new HiveOps(client)
      val meta = ops.partitionMetaData(dbName, tableName, partition).getOrError(s"Unknown partition $partition")
      meta.location
    })
  }
}
