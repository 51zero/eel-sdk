package io.eels.component.hive

import com.sksamuel.scalax.Logging
import com.typesafe.config.ConfigFactory
import io.eels.HdfsIterator
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table

import scala.collection.JavaConverters._

// returns hive paths for the table that match the partition constraints
object HiveFilesFn extends Logging {

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
        HiveFileScanner(location).map { path => path -> Partition.fromPartition(table, partition) }
      }
    }
    files.toList
  }

  // returns true if the partition meets the partition exprs
  def eval(partition: Partition, constraints: List[PartitionConstraint]): Boolean = constraints.forall(_.eval(partition))
}

object HiveFileScanner extends Logging {

  private val config = ConfigFactory.load()
  private val ignoreHiddenFiles = config.getBoolean("eel.hive.source.ignoreHiddenFiles")
  private val hiddenFilePattern = config.getString("eel.hive.source.hiddenFilePattern")

  // returns true if the given file should be considered "hidden" based on the config settings
  def isHidden(file: LocatedFileStatus): Boolean = {
    ignoreHiddenFiles && file.getPath.getName.matches(hiddenFilePattern)
  }

  def apply(location: String)(implicit fs: FileSystem): List[LocatedFileStatus] = {
    logger.debug(s"Scanning $location, filtering=$ignoreHiddenFiles, pattern=$hiddenFilePattern")
    HdfsIterator(fs.listFiles(new Path(location), true)).filter(_.isFile).filterNot(isHidden).toList
  }
}