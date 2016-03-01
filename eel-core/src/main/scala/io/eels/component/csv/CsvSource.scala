package io.eels.component.csv

import java.nio.file.Path

import com.sksamuel.scalax.io.Using
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import io.eels._

case class CsvFormat(delimiter: Char = ',', quoteChar: Char = '"', quoteEscape: Char = '"', lineSeparator: String = "\n")

case class CsvSource(path: Path,
                     overrideSchema: Option[Schema] = None,
                     format: CsvFormat = CsvFormat(),
                     inferrer: SchemaInferrer = StringInferrer,
                     hasHeader: Boolean = true) extends Source with Using {

  private def createParser: CsvParser = {
    val settings = new CsvParserSettings()
    settings.getFormat.setDelimiter(format.delimiter)
    settings.getFormat.setQuote(format.quoteChar)
    settings.getFormat.setQuoteEscape(format.quoteEscape)
    settings.setLineSeparatorDetectionEnabled(true)
    settings.setHeaderExtractionEnabled(false)
    new com.univocity.parsers.csv.CsvParser(settings)
  }

  def withSchemaInferrer(inferrer: SchemaInferrer): CsvSource = copy(inferrer = inferrer)
  def withHeader(header: Boolean): CsvSource = copy(hasHeader = header)
  def withSchema(schema: Schema): CsvSource = copy(overrideSchema = Some(schema))
  def withDelimiter(c: Char): CsvSource = copy(format = format.copy(delimiter = c))
  def withQuoteChar(c: Char): CsvSource = copy(format = format.copy(quoteChar = c))
  def withQuoteEscape(c: Char): CsvSource = copy(format = format.copy(quoteEscape = c))
  def withFormat(format: CsvFormat): CsvSource = copy(format = format)

  override def schema: Schema = overrideSchema.getOrElse {
    val parser = createParser
    parser.beginParsing(path.toFile)
    val headers = parser.parseNext.toSeq
    parser.stopParsing()
    if (hasHeader)
      inferrer(headers)
    else
      inferrer(List.tabulate(headers.size)(_.toString))
  }

  override def readers: Seq[Reader] = {
    val parser = createParser
    parser.beginParsing(path.toFile)
    val reader = new Reader {
      override def close(): Unit = parser.stopParsing()
      override def iterator: Iterator[InternalRow] = {
        val k = if (hasHeader) 1 else 0
        Iterator.continually(parser.parseNext).drop(k).takeWhile(_ != null).map(_.toSeq)
      }
    }
    Seq(reader)
  }
}

trait SchemaInferrer {
  def apply(headers: Seq[String]): Schema
}

object StringInferrer extends SchemaInferrer {
  def apply(headers: Seq[String]): Schema = Schema(headers.map(header => Column(header, SchemaType.String, true)).toList)
}

case class SchemaRule(pattern: String, schemaType: SchemaType, nullable: Boolean = true) {
  def apply(header: String): Option[Column] = {
    if (header.matches(pattern)) Some(Column(header, schemaType, nullable)) else None
  }
}

object SchemaInferrer {

  import com.sksamuel.scalax.OptionImplicits._

  def apply(default: SchemaType, first: SchemaRule, rest: SchemaRule*): SchemaInferrer = new SchemaInferrer {
    val rules = first +: rest
    override def apply(headers: Seq[String]): Schema = {
      val columns = headers.map { header =>
        rules.foldLeft(none[Column]) { (schemaType, rule) =>
          schemaType.orElse(rule(header))
        }.getOrElse(Column(header, default, true))
      }
      Schema(columns.toList)
    }
  }
}