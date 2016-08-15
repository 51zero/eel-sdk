package io.eels.component.hive

import com.sksamuel.exts.Logging
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.eels.{PartitionPart, PartitionSpec}
import io.eels.schema.PartitionConstraint
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hadoop.hive.metastore.api.{Partition => HivePartition}
import scala.collection.JavaConverters._

/**
  * Locates files for a given table.
  * Connects to the hive metastore to get the partitions list (or if no partitions then just root)
  * and scans those directories.
  * Delegates the directory scan itself to a HiveFileScanner
  */
object HiveFilesFn extends Logging {

  val config: Config = ConfigFactory.load()
  val missingPartitionAction: String = config.getString("eel.hive.source.missingPartitionAction")

  // for a given table returns hadoop paths that match the partition constraints
  def apply(table: Table,
            partitionKeys: List[String] = Nil,
            partitionConstraints: List[PartitionConstraint] = Nil)
           (implicit fs: FileSystem, client: IMetaStoreClient): List[(LocatedFileStatus, PartitionSpec)] = {

    def rootScan(): List[Pair[LocatedFileStatus, PartitionSpec]] = {
      logger.debug(s"No partitions for ${table.getTableName}; performing root scan")
      val location = new Path(table.getSd.getLocation)
      HiveFileScanner(location).map { it =>
        Pair(it, PartitionSpec.empty)
      }
    }

    def partitionsScan(partitions: Seq[HivePartition]): List[(LocatedFileStatus, PartitionSpec)] = {
      logger.debug(s"partitionsScan for $partitions")
      // first we filter out any partitions that don't meet our partition constraints
      val filteredPartitions = partitions.filter { it =>
        assert(it.getValues.size == partitionKeys.size, {
          s"Cardinality of partition values (${it.getValues}) must equal partition keys ($partitionKeys)"
        })
        // for each partition we need to combine the values with the partition keys as the
        // partition objects don't contain that
        val parts = partitionKeys.zip(it.getValues.asScala).map { case (key, value) =>
          PartitionPart(key, value)
        }
        val spec = PartitionSpec(parts.toArray)
        partitionConstraints.forall { it =>
          it.eval(spec)
        }
      }

      logger.debug(s"Filtered partitions to scan for files $filteredPartitions")

      filteredPartitions.flatMap { part =>
        val location = part.getSd.getLocation
        val path = new Path(location)
        // the partition location might not actually exist, as it might have been created in the metastore only
        if (fs.exists(path)) {
          HiveFileScanner(path).map { it =>
            val parts = partitionKeys.zip(part.getValues.asScala).map { case (key, value) =>
              PartitionPart(key, value)
            }
            Pair(it, PartitionSpec(parts.toArray))
          }
        } else if (missingPartitionAction == "error") {
          throw new IllegalStateException(s"Partition [$location] was specified in the hive metastore but did not exist on disk. To disable these exceptions set eel.hive.source.missingPartitionAction=warn")
        } else if (missingPartitionAction == "warn") {
          logger.warn(s"Partition [$location] was specified in the hive metastore but did not exist on disk. To disable these warnings set eel.hive.source.missingPartitionAction=none")
          Nil
        } else {
          Nil
        }
      }.toList
    }

    // the table may or may not have partitions.
    //
    // 1. If we do have partitions then we need to scan the path of each partition
    // (and each partition may be located anywhere outside of the table root)
    //
    // 2. If we do not have partitions then we can simply scan the table root.

    // we go to the metastore as we need the locations of the partitions not the values
    val partitions = client.listPartitions(table.getDbName, table.getTableName, Short.MaxValue)
    if (partitions.isEmpty) rootScan else partitionsScan(partitions.asScala)
  }
}
