package io.eels.component.csv

import java.nio.file.Path

import com.sksamuel.scalax.io.Using
import com.typesafe.config.ConfigFactory
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import io.eels._

sealed abstract class Header
object Header {
  case object None extends Header
  case object FirstComment extends Header
  case object FirstRow extends Header
}

case class CsvSource(path: Path,
                     overrideSchema: Option[Schema] = None,
                     format: CsvFormat = CsvFormat(),
                     inferrer: SchemaInferrer = StringInferrer,
                     ignoreLeadingWhitespaces: Boolean = true,
                     ignoreTrailingWhitespaces: Boolean = true,
                     skipEmptyLines: Boolean = true,
                     emptyCellValue: Option[String] = None,
                     verifyRows: Option[Boolean] = None,
                     header: Header = Header.FirstRow) extends Source with Using {

  val config = ConfigFactory.load()
  val DefaultVerifyRows = verifyRows.getOrElse(config.getBoolean("eel.csv.verifyRows"))

  private def createParser: CsvParser = {
    val settings = new CsvParserSettings()
    settings.getFormat.setDelimiter(format.delimiter)
    settings.getFormat.setQuote(format.quoteChar)
    settings.getFormat.setQuoteEscape(format.quoteEscape)
    settings.setLineSeparatorDetectionEnabled(true)
    // this is always true as we will fetch the headers ourselves by reading first row
    settings.setHeaderExtractionEnabled(false)
    settings.setIgnoreLeadingWhitespaces(ignoreLeadingWhitespaces)
    settings.setIgnoreTrailingWhitespaces(ignoreTrailingWhitespaces)
    settings.setSkipEmptyLines(skipEmptyLines)
    settings.setCommentCollectionEnabled(true)
    settings.setEmptyValue(emptyCellValue.orNull)
    settings.setNullValue(emptyCellValue.orNull)
    new com.univocity.parsers.csv.CsvParser(settings)
  }

  def withSchemaInferrer(inferrer: SchemaInferrer): CsvSource = copy(inferrer = inferrer)
  def withHeader(header: Header): CsvSource = copy(header = header)
  def withSchema(schema: Schema): CsvSource = copy(overrideSchema = Some(schema))
  def withDelimiter(c: Char): CsvSource = copy(format = format.copy(delimiter = c))
  def withQuoteChar(c: Char): CsvSource = copy(format = format.copy(quoteChar = c))
  def withQuoteEscape(c: Char): CsvSource = copy(format = format.copy(quoteEscape = c))
  def withFormat(format: CsvFormat): CsvSource = copy(format = format)
  def withVerifyRows(verifyRows: Boolean): CsvSource = copy(verifyRows = Some(verifyRows))
  def withEmptyCellValue(emptyCellValue: String): CsvSource = copy(emptyCellValue = Some(emptyCellValue))
  def withSkipEmptyLines(skipEmptyLines: Boolean): CsvSource = copy(skipEmptyLines = skipEmptyLines)
  def withIgnoreLeadingWhitespaces(ignore: Boolean): CsvSource = copy(ignoreLeadingWhitespaces = ignore)
  def withIgnoreTrailingWhitespaces(ignore: Boolean): CsvSource = copy(ignoreTrailingWhitespaces = ignore)

  override def schema: Schema = overrideSchema.getOrElse {
    val parser = createParser
    parser.beginParsing(path.toFile)
    val headers = header match {
      case Header.None =>
        val headers = parser.parseNext
        List.tabulate(headers.size)(_.toString)
      case Header.FirstComment =>
        while (parser.getContext.lastComment == null && parser.parseNext != null) {}
        val str = Option(parser.getContext.lastComment).getOrElse("")
        str.split(format.delimiter).toSeq
      case Header.FirstRow =>
        parser.parseNext.toSeq
    }
    parser.stopParsing()
    inferrer(headers)
  }

  override def parts: Seq[Part] = {
    val verify = header match {
      case Header.None => false
      case _ => verifyRows.getOrElse(DefaultVerifyRows)
    }
    val part = new CsvPart(createParser, path, header, verify, schema)
    Seq(part)
  }
}

class CsvPart(parser: CsvParser, path: Path, header: Header, verifyRows: Boolean, schema: Schema) extends Part {

  override def reader: SourceReader = new SourceReader {
    parser.beginParsing(path.toFile)
    override def close(): Unit = parser.stopParsing()
    override def iterator: Iterator[InternalRow] = {
      val k = header match {
        case Header.FirstRow => 1
        case _ => 0
      }
      Iterator.continually(parser.parseNext).drop(k).takeWhile(_ != null).map { array =>
        if (verifyRows) {
          assert(
            array.length == schema.size,
            s"Row has ${array.length} fields but schema has ${schema.size}\nRow=${array.mkString(", ")}\nSchema=${schema.columnNames.mkString(", ")}")
        }
        array.toSeq
      }
    }
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