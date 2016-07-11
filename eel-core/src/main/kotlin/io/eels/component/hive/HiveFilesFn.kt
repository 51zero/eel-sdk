package io.eels.component.hive

import com.typesafe.config.ConfigFactory
import io.eels.util.Logging
import io.eels.schema.PartitionConstraint
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hadoop.hive.metastore.api.Partition as HivePartition

/**
 * Locates files for a given table.
 * Connects to the hive metastore to get the partitions list (or if no partitions then just root)
 * and scans those directories.
 * Delegates the directory scan itself to a HiveFileScanner
 */
object HiveFilesFn : Logging {

  val config = ConfigFactory.load()
  val missingPartitionAction = config.getString("eel.hive.source.missingPartitionAction")

  // for a given table returns hadoop paths that match the partition constraints
  operator fun invoke(table: Table,
                      partitionConstraints: List<PartitionConstraint>,
                      partitionKeys: List<PartitionKey>,
                      fs: FileSystem,
                      client: IMetaStoreClient): List<LocatedFileStatus> {

    fun rootScan(): List<LocatedFileStatus> {
      logger.debug("No partitions for ${table.tableName}; performing root scan")
      val location = Path(table.sd.location)
      return HiveFileScanner(location, fs)
    }

    fun partitionsScan(partitions: List<HivePartition>): List<LocatedFileStatus> {
      logger.debug("partitionsScan for $partitions")
      // first we filter out any partitions that don't meet our partition constraints
      val filteredPartitions = partitions.filter {
        assert(it.values.size == partitionKeys.size, { "Partition values must equal partition keys" })
        // for each partition we need to combine the values with the partition keys as the
        // partition objects don't contain that
        val parts = partitionKeys.zip(it.values).map {
          PartitionPart(it.component1().field.name, it.component2())
        }
        val spec = PartitionSpec(parts)
        partitionConstraints.all { it.eval(spec) }
      }

      logger.debug("Filtered partitions to scan for files $filteredPartitions")

      return filteredPartitions.flatMap {
        val location = it.sd.location
        val path = Path(location)
        // the partition location might not actually exist, as it might have been created in the metastore only
        when {
          fs.exists(path) ->
            HiveFileScanner(path, fs)
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

    return when {
      partitions.isEmpty() -> rootScan()
      else -> partitionsScan(partitions)
    }
  }
}
