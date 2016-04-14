package io.eels

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.File

data class FilePattern(val pattern: String,
                       val fs: FileSystem,
                       val filter: (Path) -> Boolean = { it -> true }) : Logging {

  constructor(path: Path, fs: FileSystem) : this(path.toString(), fs, { it -> true })

  constructor(path: java.nio.file.Path, fs: FileSystem) : this(path.toAbsolutePath().toString(), fs, { it -> true })

  val FileExpansionRegex = "(file:|hdfs:)?(?://)?(.*?)/\\*"

  fun isDirectory(): Boolean = pattern.endsWith("/*") || pattern.endsWith("/")
  fun isWildcard(): Boolean = pattern.endsWith("*")

  fun toPaths(): List<Path> = when {
    isDirectory() -> {
      val path = Path(pattern.removeSuffix("/*"))
        logger.debug("File expansion will check path: " + path)
      HdfsIterator(fs.listFiles(path, true)).map { it.path }.asSequence().toList().filter { fs.isFile(it) }.filter(filter)
    }
    isWildcard() -> {
      val path = Path(pattern.removeSuffix("*")).parent
      val regex = File(pattern).name.replace("*", ".*").toRegex()
        logger.debug("File expansion will check path: " + path)
      HdfsIterator(fs.listFiles(path, false)).asSequence().toList()
          .map { it.path }
          .filter { it.name.matches(regex) }
          .filter(filter)
    }
    else -> listOf(Path(pattern))
  }

  fun withFilter(p: (Path) -> Boolean): FilePattern = copy(filter = p)
}