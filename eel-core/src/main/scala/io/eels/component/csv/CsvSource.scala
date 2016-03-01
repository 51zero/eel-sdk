package io.eels.component.csv

import java.nio.file.Path

import com.sksamuel.scalax.io.Using
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import io.eels._

trait CsvFormat {
  val delimiter: Char
  val quoteChar: Char
  val escapeChar: Char
  val lineTerminator: String
}

trait DefaultCsvFormat extends CsvFormat {
  override val delimiter: Char = ','
  override val quoteChar: Char = '"'
  override val escapeChar: Char = '"'
  override val lineTerminator: String = "\n"
}

object DefaultCsvFormat extends DefaultCsvFormat

case class CsvSource(path: Path,
                     overrideSchema: Option[Schema] = None,
                     format: CsvFormat = DefaultCsvFormat,
                     inferrer: SchemaInferrer = StringInferrer,
                     hasHeader: Boolean = true) extends Source with Using {

  private def createParser: CsvParser = {
    val settings = new CsvParserSettings()
    settings.getFormat.setDelimiter(format.delimiter)
    settings.getFormat.setQuote(format.quoteChar)
    settings.getFormat.setQuoteEscape(format.escapeChar)
    settings.setLineSeparatorDetectionEnabled(true)
    settings.setHeaderExtractionEnabled(false)
    new com.univocity.parsers.csv.CsvParser(settings)
  }

  def withDelimiter(c: Char): CsvSource = copy(format = new CsvFormat {
    override val delimiter: Char = c
    override val quoteChar: Char = format.quoteChar
    override val escapeChar: Char = format.escapeChar
    override val lineTerminator: String = format.lineTerminator
  })

  def withSchemaInferrer(inferrer: SchemaInferrer): CsvSource = copy(inferrer = inferrer)
  def withHeader(header: Boolean): CsvSource = copy(hasHeader = header)
  def withSchema(schema: Schema): CsvSource = copy(overrideSchema = Some(schema))
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
        Iterator.continually(parser.parseNext).takeWhile(_ != null).drop(k).map(_.toSeq)
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