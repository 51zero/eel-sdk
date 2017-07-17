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
  */
object HiveTableFilesFn extends Logging {

  // for a given table returns hadoop paths that match the partition constraints
  def apply(dbName: String,
            tableName: String,
            tableLocation: Path,
            partitionKeys: List[String],
            partitionConstraint: Option[PartitionConstraint])
           (implicit fs: FileSystem, client: IMetaStoreClient): Seq[(LocatedFileStatus, Partition)] = {

    val ops = new HiveOps(client)

    // when we have no partitions, this will scan just the table folder directly for files
    def rootScan(): Seq[(LocatedFileStatus, Partition)] = {
      HiveFileScanner(tableLocation, false).map { it =>
        (it, Partition.empty)
      }
    }

    def partitionsScan(partitions: Seq[PartitionMetaData]): Seq[(LocatedFileStatus, Partition)] = {
      new HivePartitionScanner().scan(partitions, partitionConstraint).flatMap { case (meta, files) =>
        files.map { file =>
          file -> meta.partition
        }
      }.toSeq
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
