package io.eels

import java.io.File

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.language.implicitConversions

case class FilePattern(pattern: String,
                       filter: Option[Path => Boolean] = None)
                      (implicit fs: FileSystem) extends StrictLogging {

  val FileExpansionRegex = "(file:|hdfs:)?(?://)?(.*?)/\\*".r

  def toPaths: Seq[Path] = {
    pattern match {

      case pat if pat.endsWith("/*") =>
        val path = new Path(pattern.stripSuffix("/*"))
        logger.debug("File expansion will check path: " + path)
        HdfsIterator(fs.listFiles(path, true)).toList.map(_.getPath).filter(fs.isFile).filter(filter.getOrElse(_ => true))

      case pat if pat.endsWith("*") =>
        val path = new Path(pattern.stripSuffix("*")).getParent
        val regex = new File(pat).getName.replace("*", ".*")
        logger.debug("File expansion will check path: " + path)
        HdfsIterator(fs.listFiles(path, false)).toList
          .map(_.getPath)
          .filter(_.getName.matches(regex))
          .filter(filter.getOrElse(_ => true))

      case str => Seq(new Path(str))
    }
  }

  def withFilter(p: Path => Boolean): FilePattern = copy(filter = Some(p))
}

object FilePattern {
  implicit def toFilePattern(str: String)(implicit fs: FileSystem): FilePattern = FilePattern(str)
  implicit def toFilePattern(path: Path)(implicit fs: FileSystem): FilePattern = FilePattern(path.toString)
  implicit def toFilePattern(file: java.io.File)(implicit fs: FileSystem): FilePattern = FilePattern(file.getAbsolutePath)
  implicit def toFilePattern(path: java.nio.file.Path)(implicit fs: FileSystem): FilePattern = FilePattern(path.toFile.getAbsolutePath)
}