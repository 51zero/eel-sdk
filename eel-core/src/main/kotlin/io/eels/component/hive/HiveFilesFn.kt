import com.typesafe.config.ConfigFactory
import io.eels.util.Logging
import io.eels.component.hive.HiveFileScanner
import io.eels.component.hive.PartitionSpec
import io.eels.schema.PartitionConstraint
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hadoop.hive.metastore.api.Partition as HivePartition

object HiveFilesFn : Logging {

  val config = ConfigFactory.load()
  val errorOnMissingPartitions = config.getBoolean("eel.hive.source.errorOnMissingPartitions")
  val warnOnMissingPartitions = config.getBoolean("eel.hive.source.warnOnMissingPartitions")

  // for a given table returns hadoop paths that match the partition constraints
  operator fun invoke(table: Table,
                      partitionConstraints: List<PartitionConstraint>,
                      fs: FileSystem,
                      client: IMetaStoreClient): List<Pair<LocatedFileStatus, PartitionSpec>> {

    // returns true if the partition meets the partition constraint
    fun eval(partition: HivePartition, constraints: List<PartitionConstraint>): Boolean {
      val location = partition.sd.location
      val eelPartition = PartitionSpec.parsePath(location)
      return constraints.all { it.eval(eelPartition) }
    }

    fun rootScan(): List<Pair<LocatedFileStatus, PartitionSpec>> {
      val location = Path(table.sd.location)
      return HiveFileScanner(location, fs).map { Pair(it, PartitionSpec.empty) }
    }

    fun partitionsScan(partitions: List<HivePartition>): List<Pair<LocatedFileStatus, PartitionSpec>> {
      // first we filter out any partitions that don't meet our partition constraints
      val filteredPartitions = partitions.filter { eval(it, partitionConstraints) }
      return filteredPartitions.flatMap {
        val location = it.sd.location
        val path = Path(location)
        // the partition location might not actually exist, as it might just have been created in the metastore only
        when {
          fs.exists(path) ->
            HiveFileScanner(path, fs).map { Pair(it, PartitionSpec.parsePath(location)) }
          errorOnMissingPartitions ->
            throw IllegalStateException("Partition [$location] was specified in the hive metastore but did not exist on disk. To disable these exceptions set eel.hive.source.errorOnMissingPartitions=false")
          warnOnMissingPartitions ->
            throw IllegalStateException("Partition [$location] was specified in the hive metastore but did not exist on disk. To disable these exceptions set eel.hive.source.errorOnMissingPartitions=false")
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
