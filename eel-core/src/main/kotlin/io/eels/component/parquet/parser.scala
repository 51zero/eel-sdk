package io.eels.component.parquet

import java.nio.file.Paths

import io.eels.{SinkParser, SourceParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf

object ParquetSourceParser extends SourceParser {
  val regex = "parquet:([^?].*?)(\\?.*)?".r
  override def apply(str: String): Option[Builder[ParquetSource]] = str match {
    case regex(path, params) =>
      Some(ParquetSourceBuilder(path, Option(params).map(UrlParamParser.apply).getOrElse(Map.empty)))
    case _ => None
  }
}

case class ParquetSourceBuilder(path: String, params: Map[String, List[String]]) extends Builder[ParquetSource] {
  require(path != null, "path cannot be null")
  override def apply(): ParquetSource = {
    implicit val fs = FileSystem.get(new Configuration)
    new ParquetSource(Paths.get(path))
  }
}

object ParquetSinkParser extends SinkParser {
  val Regex = "parquet:([^?].*?)(\\?.*)?".r
  override def apply(str: String): Option[Builder[ParquetSink]] = str match {
    case Regex(path, params) =>
      Some(ParquetSinkBuilder(new Path(path), Option(params).map(UrlParamParser.apply).getOrElse(Map.empty)))
    case _ => None
  }
}

case class ParquetSinkBuilder(path: Path, params: Map[String, List[String]]) extends Builder[ParquetSink] {
  require(path != null, "path name cannot be null")
  override def apply(): ParquetSink = {
    implicit val fs = FileSystem.get(new Configuration)
    implicit val conf = new HiveConf()
    new ParquetSink(path)
  }
}