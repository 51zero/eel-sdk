package io.eels.component.hive

import com.sksamuel.exts.Logging
import io.eels.component.hive.partition.PartitionMetaData
import io.eels.schema.{Partition, PartitionConstraint}
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
  * Locates files for a given table.
  *
  * Connects to the hive metastore to get the partitions list (or if no partitions then just root)
  * and scans those directories.
  *
  * Returns a Map of each partition to the files in that partition.
  *
  * If partition constraints are specified then those partitions are filtered out.
  */
object HiveTableFilesFn extends Logging {

  def apply(dbName: String,
            tableName: String,
            tableLocation: Path,
            partitionConstraints: Seq[PartitionConstraint])
           (implicit fs: FileSystem, client: IMetaStoreClient): Map[Partition, Seq[LocatedFileStatus]] = {

    val ops = new HiveOps(client)

    // when we have no partitions, this will scan just the table folder directly for files
    def rootScan(): Map[Partition, Seq[LocatedFileStatus]] = {
      Map(Partition.empty -> HiveFileScanner(tableLocation, false))
    }

    def partitionsScan(partitions: Seq[PartitionMetaData]): Map[Partition, Seq[LocatedFileStatus]] = {
      new HivePartitionScanner().scan(partitions, partitionConstraints)
        .map { case (key, value) => key.partition -> value }
    }

    // the table may or may not have partitions.
    //
    // 1. If we do have partitions then we need to scan the path of each partition
    // (and each partition may be located anywhere outside of the table root)
    //
    // 2. If we do not have partitions then we can simply scan the table root.

    // we go to the metastore as we need the locations of the partitions not the values
    val partitions = ops.partitionsMetaData(dbName, tableName)
    if (partitions.isEmpty) {
      logger.debug(s"No partitions for $tableName; performing root table scan")
      rootScan
    } else partitionsScan(partitions)
  }
}
