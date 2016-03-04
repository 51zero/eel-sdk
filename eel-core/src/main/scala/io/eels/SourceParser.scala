package io.eels

import io.eels.component.Builder
import io.eels.component.hive.HiveSourceParser

import scala.collection.mutable.ListBuffer
import com.sksamuel.scalax.OptionImplicits._
import io.eels.component.avro.AvroSourceParser
import io.eels.component.csv.CsvSourceParser
import io.eels.component.parquet.ParquetSourceParser

trait SourceParser {
  def apply(url: String): Option[Builder[Source]]
}

trait SinkParser[T <: Sink] {
  def apply(url: String): Option[Builder[T]]
}

object SourceParser {
  val parsers = new ListBuffer[SourceParser]
  parsers.append(HiveSourceParser)
  parsers.append(CsvSourceParser)
  parsers.append(ParquetSourceParser)
  parsers.append(AvroSourceParser)
  def apply(url: String): Option[Builder[Source]] = {
    parsers.foldLeft(none[Builder[Source]])((a, parser) => a.orElse(parser(url)))
  }
}
