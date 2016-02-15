package io.eels.component.hive

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.HdfsIterator
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.hadoop.hive.metastore.api.Table

object HiveFileScanner {
  def apply(table: Table, partitionExprs: List[PartitionExpr])(implicit fs: FileSystem): List[Path] = {
    if (partitionExprs.isEmpty) HiveTableFileEnumerator(table)
    else HivePartitionFileEnumerator(table, partitionExprs)
  }
}

trait HiveFileScanner extends StrictLogging {

  protected val config = ConfigFactory.load()
  protected val ignoreHiddenFiles = config.getBoolean("eel.hive.source.ignoreHiddenFiles")
  protected val hiddenFilePattern = config.getString("eel.hive.source.hiddenFilePattern")

  // returns true if the given file should be considered "hidden" based on the config settings
  def isHidden(file: LocatedFileStatus): Boolean = {
    ignoreHiddenFiles && file.getPath.getName.matches(hiddenFilePattern)
  }

  // returns all files in the given location that are visible
  def scan(location: String)(implicit fs: FileSystem): List[LocatedFileStatus] = {
    logger.debug(s"Scanning $location, filtering=$ignoreHiddenFiles, pattern=$hiddenFilePattern")
    val files = HdfsIterator(fs.listFiles(new Path(location), true)).filter(_.isFile).filterNot(isHidden).toList
    logger.debug(s"Found ${files.size} visible files")
    files
  }
}

// loads all files under the table location
object HiveTableFileEnumerator extends HiveFileScanner {
  def apply(table: Table)(implicit fs: FileSystem): List[Path] = {
    val location = table.getSd.getLocation
    scan(location).map(_.getPath)
  }
}

// loads all files under a table for the given set of partitions
object HivePartitionFileEnumerator extends HiveFileScanner {

  def apply(table: Table, partitionExprs: List[PartitionExpr])(implicit fs: FileSystem): List[Path] = {

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

    val location = table.getSd.getLocation

    // returns all files in the given location that are both visible and that match the partition expressions
    // todo this doesn't take into account partitions that are not stored under the table location
    scan(location).filter(isEvaluated).map(_.getPath)
  }
}