package io.eels.component.avro

import java.nio.file.Paths

import com.sksamuel.scalax.net.UrlParamParser
import io.eels.SourceParser
import io.eels.component.Builder

object AvroSourceParser extends SourceParser {
  val regex = "avro:([^?].*?)(\\?.*)?".r
  override def apply(str: String): Option[Builder[AvroSource]] = str match {
    case regex(path, params) =>
      Some(AvroSourceBuilder(path, Option(params).map(UrlParamParser.apply).getOrElse(Map.empty)))
    case _ => None
  }
}

case class AvroSourceBuilder(path: String, params: Map[String, List[String]]) extends Builder[AvroSource] {
  require(path != null, "path cannot be null")
  override def apply(): AvroSource = new AvroSource(Paths.get(path))
}