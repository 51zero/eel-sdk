package io.eels.component.csv

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.univocity.parsers.csv.CsvParser
import com.univocity.parsers.csv.CsvParserSettings
import io.eels.schema.Schema
import io.eels.Source
import io.eels.component.Part
import io.eels.component.SchemaInferrer
import io.eels.component.StringInferrer
import io.eels.component.Using
import io.eels.util.Option
import io.eels.util.getOrElse
import java.nio.file.Path

enum class Header {
  None, FirstComment, FirstRow
}

data class CsvSource @JvmOverloads constructor(val path: Path,
                                               val overrideSchema: Option<Schema> = Option.None,
                                               val format: CsvFormat = CsvFormat(),
                                               val inferrer: SchemaInferrer = StringInferrer,
                                               val ignoreLeadingWhitespaces: Boolean = true,
                                               val ignoreTrailingWhitespaces: Boolean = true,
                                               val skipEmptyLines: Boolean = true,
                                               val emptyCellValue: String? = null,
                                               val nullValue: String? = null,
                                               val verifyRows: Option<Boolean> = Option.None,
                                               val header: Header = Header.FirstRow) : Source, Using {

  val config: Config = ConfigFactory.load()
  val defaultVerifyRows = verifyRows.getOrElse(config.getBoolean("eel.csv.verifyRows"))

  private fun createParser(): CsvParser {
    val settings = CsvParserSettings()
    settings.format.delimiter = format.delimiter
    settings.format.quote = format.quoteChar
    settings.format.quoteEscape = format.quoteEscape
    settings.isLineSeparatorDetectionEnabled = true
    // this is always true as we will fetch the headers ourselves by reading first row
    settings.isHeaderExtractionEnabled = false
    settings.ignoreLeadingWhitespaces = ignoreLeadingWhitespaces
    settings.ignoreTrailingWhitespaces = ignoreTrailingWhitespaces
    settings.skipEmptyLines = skipEmptyLines
    settings.isCommentCollectionEnabled = true
    settings.emptyValue = emptyCellValue
    settings.nullValue = nullValue
    return com.univocity.parsers.csv.CsvParser(settings)
  }

  fun withSchemaInferrer(inferrer: SchemaInferrer): CsvSource = copy(inferrer = inferrer)

  // sets whether this source has a header and if so where to read from
  fun withHeader(header: Header): CsvSource = copy(header = header)

  fun withSchema(schema: Schema): CsvSource = copy(overrideSchema = Option.Some(schema))
  fun withDelimiter(c: Char): CsvSource = copy(format = format.copy(delimiter = c))
  fun withQuoteChar(c: Char): CsvSource = copy(format = format.copy(quoteChar = c))
  fun withQuoteEscape(c: Char): CsvSource = copy(format = format.copy(quoteEscape = c))
  fun withFormat(format: CsvFormat): CsvSource = copy(format = format)
  fun withVerifyRows(verifyRows: Boolean): CsvSource = copy(verifyRows = Option.Some(verifyRows))

  // use this value when the cell/record is empty quotes in the source data
  fun withEmptyCellValue(emptyCellValue: String?): CsvSource = copy(emptyCellValue = emptyCellValue)

  // use this value when the cell/record is empty in the source data
  fun withNullValue(nullValue: String?): CsvSource = copy(nullValue = nullValue)

  fun withSkipEmptyLines(skipEmptyLines: Boolean): CsvSource = copy(skipEmptyLines = skipEmptyLines)
  fun withIgnoreLeadingWhitespaces(ignore: Boolean): CsvSource = copy(ignoreLeadingWhitespaces = ignore)
  fun withIgnoreTrailingWhitespaces(ignore: Boolean): CsvSource = copy(ignoreTrailingWhitespaces = ignore)

  override fun schema(): Schema = overrideSchema.getOrElse {
    val parser = createParser()
    parser.beginParsing(path.toFile())
    val headers = when (header) {
      Header.None -> {
        // read the first row just to get the count of columns, then we'll call them column 1,2,3,4 etc
        // todo change the column labels to a,b,c,d
        val records = parser.parseNext()
        (0..records.size - 1).map { it.toString() }.toList()
      }
      Header.FirstComment -> {
        while (parser.context.lastComment() == null && parser.parseNext() != null) {
        }
        val str = Option(parser.context.lastComment()).getOrElse("")
        str.split(format.delimiter).toList()
      }
      Header.FirstRow -> parser.parseNext().toList()
    }
    parser.stopParsing()
    inferrer.schemaOf(headers)
  }

  override fun parts(): List<Part> {
    val verify = when (header) {
      Header.None -> false
      else -> verifyRows.getOrElse(defaultVerifyRows)
    }
    val part = CsvPart({ createParser() }, path, header, verify, schema())
    return listOf(part)
  }
}