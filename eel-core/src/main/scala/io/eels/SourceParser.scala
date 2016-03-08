package io.eels

import io.eels.component.Builder
import io.eels.component.hive.{HiveSinkParser, HiveSourceParser}

import scala.collection.mutable.ListBuffer
import com.sksamuel.scalax.OptionImplicits._
import io.eels.component.avro.AvroSourceParser
import io.eels.component.csv.{CsvSinkParser, CsvSourceParser}
import io.eels.component.parquet.{ParquetSinkParser, ParquetSourceParser}

trait SourceParser {
  def apply(url: String): Option[Builder[Source]]
}

trait SinkParser {
  def apply(url: String): Option[Builder[Sink]]
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

object SinkParser {
  val parsers = new ListBuffer[SinkParser]
  parsers.append(HiveSinkParser)
  parsers.append(CsvSinkParser)
  parsers.append(ParquetSinkParser)
  def apply(url: String): Option[Builder[Sink]] = {
    parsers.foldLeft(none[Builder[Sink]])((a, parser) => a.orElse(parser(url)))
  }
}