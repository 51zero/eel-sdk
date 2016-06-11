import com.typesafe.config.ConfigFactory
import io.eels.Logging
import io.eels.component.hive.HiveFileScanner
import io.eels.component.hive.Partition
import io.eels.component.hive.PartitionConstraint
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table

// returns hive paths for the table that match the partition constraints
object HiveFilesFn : Logging {

  private val config = ConfigFactory.load()
  private val errorOnMissingPartitions = config.getBoolean("eel.hive.source.errorOnMissingPartitions")
  private val warnOnMissingPartitions = config.getBoolean("eel.hive.source.warnOnMissingPartitions")

  fun apply(table: Table, partitionConstraints: List<PartitionConstraint>, fs: FileSystem, client: IMetaStoreClient): List<Pair<LocatedFileStatus, Partition>> {

    // the table may or may not have partitions. If we do have partitions then we need to scan inside of the
    // locations of each partition (and each partition may be located anywhere outside of the table root).
    // If we do not have partitions then we can simply scan the table root.
    val partitions = client.listPartitions(table.dbName, table.tableName, Short.MAX_VALUE)

    val files = if (partitions.isEmpty()) {
      val location = table.sd.location
      HiveFileScanner(location, fs).map { Pair(it, Partition.Empty()) }
    } else {
      // first we filter out any partitions that don't meet our partition constraints
      partitions.filter {
        val p = Partition.fromPartition(table, it)
        eval(p, partitionConstraints)
      }.flatMap {
        val location = it.sd.location
        // the partition location might not actually exist, as it might just be in the metastore
        val exists = fs.exists(Path(location))
        if (exists) {
          HiveFileScanner(location, fs).map { Pair(it, Partition.fromPartition(table, it)) }
        } else if (errorOnMissingPartitions) {
          error("Partition path $location does not exist")
        } else {
          if (warnOnMissingPartitions) {
            logger.warn("Partition path $location does not exist and will be skipped")
          }
          listOf()
        }
      }
    }
    return files.toList()
  }

  // returns true if the partition meets the partition exprs
  fun eval(partition: Partition, constraints: List<PartitionConstraint>): Boolean = constraints.all { it.eval(partition) }
}
