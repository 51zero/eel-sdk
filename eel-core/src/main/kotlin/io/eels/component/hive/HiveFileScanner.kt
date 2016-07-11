package io.eels.component.hive

import com.typesafe.config.ConfigFactory
import io.eels.HdfsIterator
import io.eels.util.Logging
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path

object HiveFileScanner : Logging {

  private val config = ConfigFactory.load()
  private val ignoreHiddenFiles = config.getBoolean("eel.hive.source.ignoreHiddenFiles")
  private val hiddenFilePattern = config.getString("eel.hive.source.hiddenFilePattern")

  // returns true if the given file should be considered based on the config settings
  fun skip(file: LocatedFileStatus): Boolean {
    return file.len == 0L || ignoreHiddenFiles && file.path.name.matches(hiddenFilePattern.toRegex())
  }

  // given a hadoop path, will look for files inside that path that match the
  // configured settings for hidden files
  operator fun invoke(path: Path, fs: FileSystem): List<LocatedFileStatus> {
    logger.debug("Scanning $path, filtering=$ignoreHiddenFiles, pattern=$hiddenFilePattern")
    val files: List<LocatedFileStatus> = if (fs.exists(path)) {
      val files = fs.listFiles(path, true)
      HdfsIterator(files)
          .asSequence()
          .filter { it.isFile }
          .filterNot { skip(it) }
          .toList()
    } else {
      emptyList<LocatedFileStatus>()
    }
    return files
  }
}
