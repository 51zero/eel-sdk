package io.eels

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.language.implicitConversions

case class FilePattern(pattern: String) extends StrictLogging {
  val FileExpansionRegex = "(file|hdfs):(?://)?(.*?)/\\*".r
  def toPaths: Seq[Path] = {
    pattern match {
      case FileExpansionRegex(_, _) =>
        val path = new Path(pattern.stripSuffix("/*"))
        logger.debug("File expansion will check path: " + path)
        val fs = FileSystem.get(new Configuration)
        HdfsIterator(fs.listFiles(path, false)).toList.map(_.getPath)
      case str => Seq(new Path(str))
    }
  }
}
