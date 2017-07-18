package io.eels.component.hive

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.util.HdfsIterator
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}

// given a hadoop path, will look for files inside that path that match the
// configured settings for hidden files
// does not return directories
object HiveFileScanner extends Logging {

  private val config = ConfigFactory.load()
  private val ignoreHiddenFiles = config.getBoolean("eel.hive.source.ignoreHiddenFiles")
  private val hiddenFilePattern = config.getString("eel.hive.source.hiddenFilePattern")

  // returns true if the given file should be considered based on the config settings
  private def skip(file: LocatedFileStatus): Boolean = {
    file.getLen == 0L || ignoreHiddenFiles && file.getPath.getName.matches(hiddenFilePattern)
  }

  def apply(path: Path, recursive: Boolean)(implicit fs: FileSystem): Seq[LocatedFileStatus] = {
    logger.debug(s"Scanning $path, filtering=$ignoreHiddenFiles, pattern=$hiddenFilePattern")
    val files: List[LocatedFileStatus] = if (fs.exists(path)) {
      val files = fs.listFiles(path, recursive)
      HdfsIterator.remote(files)
          .filter(_.isFile)
          .filterNot(skip)
          .toList
    } else {
      Nil
    }
    logger.debug(s"Scanner found ${files.size} files")
    files
  }
}
