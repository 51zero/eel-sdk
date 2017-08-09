package io.eels

import com.sksamuel.exts.Logging
import io.eels.util.HdfsIterator
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.language.implicitConversions

object FilePattern {
  def apply(path: Path)(implicit fs: FileSystem): FilePattern = apply(path.toString())
  def apply(path: java.nio.file.Path)(implicit fs: FileSystem): FilePattern = apply(path.toAbsolutePath().toString(), { _ => true })

  implicit def stringToFilePattern(str: String)(implicit fs: FileSystem): FilePattern = FilePattern(str)
}

case class FilePattern(pattern: String,
                       filter: org.apache.hadoop.fs.Path => Boolean = { _ => true }) extends Logging {

  def isRegex(): Boolean = pattern.contains("*")
  def isDirectory(): Boolean = pattern.endsWith("/")

  def toPaths()(implicit fs: FileSystem): List[Path] = {
    val paths = if (isRegex) {

      val regex = new Path(pattern).getName.replace("*", ".*?")
      val dir = new Path(pattern).getParent
      logger.debug(s"File expansion will check path $dir for files matching $regex")

      HdfsIterator.remote(fs.listFiles(dir, false)).toList
        .map(_.getPath)
        .filter { path => path.getName.matches(regex) }
        .filter(filter)

    } else if (fs.isDirectory(new Path(pattern))) {

      val path = new Path(pattern.stripSuffix("/"))
      logger.debug(s"File expansion will search directory $path")
      HdfsIterator.remote(fs.listFiles(path, false)).map(_.getPath).toList.filter(fs.isFile).filter(filter)

    } else {
      List(new Path(pattern))
    }

    logger.debug(s"toPaths has returned ${paths.size} paths, first 5: ${paths.take(5).mkString(",")}")
    paths
  }

  def withFilter(p: Path => Boolean): FilePattern = copy(filter = p)
}