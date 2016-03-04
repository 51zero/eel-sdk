package io.eels.component.parquet

import java.nio.file.Paths

import com.sksamuel.scalax.net.UrlParamParser
import io.eels.SourceParser
import io.eels.component.Builder

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
  override def apply(): ParquetSource = new ParquetSource(Paths.get(path))
}