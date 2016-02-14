package io.eels

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.language.implicitConversions

case class FilePattern(pattern: String, filter: Option[Path => Boolean] = None) extends StrictLogging {
  val FileExpansionRegex = "(file:|hdfs:)?(?://)?(.*?)/\\*".r
  def toPaths: Seq[Path] = {
    pattern match {
      case pat if pat.endsWith("/*") =>
        val path = new Path(pattern.stripSuffix("/*"))
        logger.debug("File expansion will check path: " + path)
        val fs = FileSystem.get(new Configuration)
        HdfsIterator(fs.listFiles(path, false)).toList.map(_.getPath).filter(filter.getOrElse(_ => true))
      case str => Seq(new Path(str))
    }
  }
  def withFilter(p: Path => Boolean): FilePattern = copy(filter = Some(p))
}

object FilePattern {
  implicit def toFilePattern(str: String): FilePattern = FilePattern(str)
  implicit def toFilePattern(path: Path): FilePattern = FilePattern(path.toString)
  implicit def toFilePattern(file: java.io.File): FilePattern = FilePattern(file.getAbsolutePath)
  implicit def toFilePattern(path: java.nio.file.Path): FilePattern = FilePattern(path.toFile.getAbsolutePath)
}