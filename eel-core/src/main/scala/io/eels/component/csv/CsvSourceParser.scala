package io.eels.component.csv

import java.nio.file.Paths

import com.sksamuel.scalax.net.UrlParamParser
import io.eels.SourceParser
import io.eels.component.SourceBuilder

object CsvSourceParser extends SourceParser[CsvSource] {
  val CsvRegex = "csv:([^?].*?)(\\?.*)?".r
  override def apply(str: String): Option[SourceBuilder[CsvSource]] = str match {
    case CsvRegex(path, params) =>
      Some(CsvSourceBuilder(path, Option(params).map(UrlParamParser.apply).getOrElse(Map.empty)))
    case _ => None
  }
}

case class CsvSourceBuilder(path: String, params: Map[String, List[String]])
  extends SourceBuilder[CsvSource] {
  require(path != null, "path name cannot be null")
  override def apply: CsvSource = new CsvSource(Paths.get(path))
}