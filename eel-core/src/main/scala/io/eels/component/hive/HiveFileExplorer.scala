package io.eels.component.hive

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.HdfsIterator
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.hadoop.hive.metastore.api.Table

object HiveFileExplorer extends StrictLogging {

  val config = ConfigFactory.load()
  val ignoreHiddenFiles = config.getBoolean("eel.hive.source.ignoreHiddenFiles")
  val hiddenFilePattern = config.getString("eel.hive.source.hiddenFilePattern")

  def apply(table: Table, partitionExprs: List[PartitionExpr])
           (implicit fs: FileSystem): List[Path] = {

    // returns true if the given file should be considered "hidden" based on the config settings
    def isHidden(file: LocatedFileStatus): Boolean = {
      ignoreHiddenFiles && file.getPath.getName.matches(hiddenFilePattern)
    }

    // returns true if the file meets the partition exprs
    def isEvaluated(file: LocatedFileStatus): Boolean = {
      // todo need better way of getting all partition info
      // todo this doesn't take into account partitions that are not stored under key=value semantics
      val partitions = Iterator.iterate(file.getPath.getParent)(path => path.getParent)
        .takeWhile(_ != null)
        .filter(_.getName.contains("="))
        .collect {
          case PartitionPart(name, value) => PartitionPart(name, value)
        }.toList
      partitionExprs.forall(_.eval(partitions))
    }

    // returns all files in the given location that are both visible and that match the partition expressions
    // todo this doesn't take into account partitions that are not stored under the table location
    def paths(location: String): List[Path] = {
      logger.debug(s"Scanning $location, filtering=$ignoreHiddenFiles, pattern=$hiddenFilePattern")
      val files = HdfsIterator(fs.listFiles(new Path(location), true)).filter(_.isFile).toList
      logger.debug(s"Found ${files.size} files before filtering")

      val visible = files.filterNot(isHidden).filter(isEvaluated).map(_.getPath)
      logger.info(s"Found ${visible.size} files after filtering")
      visible
    }

    val location = table.getSd.getLocation
    paths(location)
  }
}
