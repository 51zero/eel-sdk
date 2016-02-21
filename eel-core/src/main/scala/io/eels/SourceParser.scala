package io.eels

import io.eels.component.Builder
import io.eels.component.hive.HiveSourceParser

import scala.collection.mutable.ListBuffer
import com.sksamuel.scalax.OptionImplicits._

trait SourceParser[T <: Source] {
  def apply(url: String): Option[Builder[T]]
}

trait SinkParser[T <: Sink] {
  def apply(url: String): Option[Builder[T]]
}

object SourceParser {
  val parsers = new ListBuffer[SourceParser[_]]
  parsers.append(HiveSourceParser)
  def apply(url: String): Option[Builder[_]] = {
    parsers.foldLeft(none[Builder[_]])((a, parser) => a.orElse(parser(url)))
  }
}
