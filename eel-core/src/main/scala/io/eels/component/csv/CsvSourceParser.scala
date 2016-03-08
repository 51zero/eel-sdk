package io.eels.component.csv

import java.nio.file.Paths

import com.sksamuel.scalax.net.UrlParamParser
import io.eels.{SinkParser, SourceParser}
import io.eels.component.Builder

object CsvSourceParser extends SourceParser {
  val Regex = "csv:([^?].*?)(\\?.*)?".r
  override def apply(str: String): Option[Builder[CsvSource]] = str match {
    case Regex(path, params) =>
      Some(CsvSourceBuilder(path, Option(params).map(UrlParamParser.apply).getOrElse(Map.empty)))
    case _ => None
  }
}

case class CsvSourceBuilder(path: String, params: Map[String, List[String]]) extends Builder[CsvSource] {
  require(path != null, "path name cannot be null")
  override def apply(): CsvSource = new CsvSource(Paths.get(path))
}

object CsvSinkParser extends SinkParser {
  val Regex = "csv:([^?].*?)(\\?.*)?".r
  override def apply(str: String): Option[Builder[CsvSink]] = str match {
    case Regex(path, params) =>
      Some(CsvSinkBuilder(path, Option(params).map(UrlParamParser.apply).getOrElse(Map.empty)))
    case _ => None
  }
}

case class CsvSinkBuilder(path: String, params: Map[String, List[String]]) extends Builder[CsvSink] {
  require(path != null, "path name cannot be null")
  override def apply(): CsvSink = new CsvSink(Paths.get(path))
}