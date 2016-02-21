package io.eels

import io.eels.component.SourceBuilder
import io.eels.component.hive.HiveSourceParser

import scala.collection.mutable.ListBuffer
import com.sksamuel.scalax.OptionImplicits._

trait SourceParser[T <: Source] {
  def apply(url: String): Option[SourceBuilder[T]]
}

object SourceParser {
  val parsers = new ListBuffer[SourceParser[_]]
  parsers.append(HiveSourceParser)
  def apply(url: String): Option[SourceBuilder[_]] = {
    parsers.foldLeft(none[SourceBuilder[_]])((a, parser) => a.orElse(parser(url)))
  }
}
