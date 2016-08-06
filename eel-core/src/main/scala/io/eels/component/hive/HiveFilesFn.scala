package io.eels.component.hive

import com.sksamuel.exts.Logging
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.eels.{PartitionPart, PartitionSpec}
import io.eels.util.Logging
import io.eels.schema.PartitionConstraint
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hadoop.hive.metastore.api.Partition

import scala.reflect.io.Path

as HivePartition

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
            fs: FileSystem,
            client: IMetaStoreClient,
            partitionKeys: List[String],
            partitionConstraints: List[PartitionConstraint] = Nil): List[(LocatedFileStatus, PartitionSpec)] = {

    def rootScan(): List[Pair[LocatedFileStatus, PartitionSpec]] = {
      logger.debug(s"No partitions for ${table.getTableName}; performing root scan")
      val location = Path(table.getSd.getLocation)
      new HiveFileScanner(location, fs).map { it =>
        Pair(it, PartitionSpec.empty)
      }
    }

    def partitionsScan(partitions: List[HivePartition]): List[(LocatedFileStatus, PartitionSpec)] = {
      logger.debug("partitionsScan for $partitions")
      // first we filter out any partitions that don't meet our partition constraints
      val filteredPartitions = partitions.filter {
        assert(it.values.size == partitionKeys.size, {
          "Cardinality of partition values (${it.values}) must equal partition keys (${partitionKeys})"
        })
        // for each partition we need to combine the values with the partition keys as the
        // partition objects don't contain that
        val parts = partitionKeys.zip(it.values).map {
          PartitionPart(it.component1(), it.component2())
        }
        val spec = PartitionSpec(parts)
        partitionConstraints.all {
          it.eval(spec)
        }
      }

      logger.debug(s"Filtered partitions to scan for files $filteredPartitions")

      filteredPartitions.flatMap {
        part ->
        val location = part.sd.location
        val path = Path(location)
        // the partition location might not actually exist, as it might have been created in the metastore only
        when {
          fs.exists(path) ->
            HiveFileScanner(path, fs).map {
              val parts = partitionKeys.zip(part.values).map {
                PartitionPart(it.first, it.second)
              }
              Pair(it, PartitionSpec(parts))
            }
          missingPartitionAction == "error" ->
          throw IllegalStateException("Partition [$location] was specified in the hive metastore but did not exist on disk. To disable these exceptions set eel.hive.source.missingPartitionAction=warn")
          missingPartitionAction == "warn" -> {
            logger.warn("Partition [$location] was specified in the hive metastore but did not exist on disk. To disable these warnings set eel.hive.source.missingPartitionAction=none")
            emptyList()
          }
          else -> emptyList()
        }
      }
    }

    // the table may or may not have partitions.
    //
    // 1. If we do have partitions then we need to scan the path of each partition
    // (and each partition may be located anywhere outside of the table root)
    //
    // 2. If we do not have partitions then we can simply scan the table root.

    // we go to the metastore as we need the locations of the partitions not the values
    val partitions = client.listPartitions(table.dbName, table.tableName, Short.MAX_VALUE)

    if (partitions.isEmpty) rootScan else partitionsScan(partitions)

  }
}
