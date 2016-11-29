package io.eels

import java.io.File

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

  val FileExpansionRegex = "(file:|hdfs:)?(?://)?(.*?)/\\*"

  def isDirectory(): Boolean = pattern.endsWith("/*") || pattern.endsWith("/")
  def isWildcard(): Boolean = pattern.endsWith("*")

  def toPaths()(implicit fs: FileSystem): List[Path] =
    if (isDirectory()) {
      val path = new Path(pattern.stripSuffix("/*"))
      logger.debug("File expansion will check path: " + path)
      HdfsIterator(fs.listFiles(path, true)).map(_.getPath).toList.filter(fs.isFile).filter(filter)
    } else if (isWildcard()) {
      val path = new Path(pattern.stripSuffix("*")).getParent
      val regex = new File(pattern).getName.replace("*", ".*")
      logger.debug("File expansion will check path: " + path)
      HdfsIterator(fs.listFiles(path, false)).toList.map(_.getPath).filter(_.getName.matches(regex)).filter(filter)
    } else {
      List(new Path(pattern))
    }

  def withFilter(p: Path => Boolean): FilePattern = copy(filter = p)
}