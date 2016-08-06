package io.eels

import java.io.File

import com.sksamuel.exts.Logging
import org.apache.hadoop.fs.{FileSystem, Path}

object FilePattern {
  def apply(path: Path, fs: FileSystem): FilePattern = apply(path.toString(), fs)
  def apply(path: java.nio.file.Path, fs: FileSystem): FilePattern = apply(path.toAbsolutePath().toString(), fs, { _ => true })
}

case class FilePattern(pattern: String,
                       fs: FileSystem,
                       filter: org.apache.hadoop.fs.Path => Boolean = { _ => true }) extends Logging {

  val FileExpansionRegex = "(file:|hdfs:)?(?://)?(.*?)/\\*"

  def isDirectory(): Boolean = pattern.endsWith("/*") || pattern.endsWith("/")
  def isWildcard(): Boolean = pattern.endsWith("*")

  def toPaths(): List[Path] =
    if (isDirectory()) {
      val path = new Path(pattern.stripSuffix("/*"))
      logger.debug("File expansion will check path: " + path)
      HdfsIterator(fs.listFiles(path, true)).map(_.getPath).toList.filter(p => fs.isFile(p)).filter(filter)
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