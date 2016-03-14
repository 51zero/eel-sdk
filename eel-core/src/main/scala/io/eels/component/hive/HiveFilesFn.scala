package io.eels.component.hive

import com.sksamuel.scalax.Logging
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table

import scala.collection.JavaConverters._

// returns hive paths for the table that match the partition constraints
object HiveFilesFn extends Logging {

  private val config = ConfigFactory.load()
  private val errorOnMissingPartitions = config.getBoolean("eel.hive.source.errorOnMissingPartitions")
  private val warnOnMissingPartitions = config.getBoolean("eel.hive.source.warnOnMissingPartitions")

  def apply(table: Table, partitionConstraints: List[PartitionConstraint])
           (implicit fs: FileSystem, client: IMetaStoreClient): List[(LocatedFileStatus, Partition)] = {

    // the table may or may not have partitions. If we do have partitions then we need to scan inside of the
    // locations of each partition (and each partition may be located anywhere outside of the table root).
    // If we do not have partitions then we can simply scan the table root.
    val partitions = client.listPartitions(table.getDbName, table.getTableName, Short.MaxValue).asScala

    val files = if (partitions.isEmpty) {
      val location = table.getSd.getLocation
      HiveFileScanner(location).map(path => path -> Partition.Empty)
    } else {
      // first we filter out any partitions that don't meet our partition constraints
      partitions.filter { partition =>
        val p = Partition.fromPartition(table, partition)
        eval(p, partitionConstraints)
      }.flatMap { partition =>
        val location = partition.getSd.getLocation
        // the partition location might not actually exist, as it might just be in the metastore
        val exists = fs.exists(new Path(location))
        if (exists) {
          HiveFileScanner(location).map { path => path -> Partition.fromPartition(table, partition) }
        } else if (errorOnMissingPartitions) {
          sys.error(s"Partition path $location does not exist")
        } else {
          if (warnOnMissingPartitions) {
            logger.warn(s"Partition path $location does not exist and will be skipped")
          }
          Nil
        }
      }
    }
    files.toList
  }

  // returns true if the partition meets the partition exprs
  def eval(partition: Partition, constraints: List[PartitionConstraint]): Boolean = constraints.forall(_.eval(partition))
}