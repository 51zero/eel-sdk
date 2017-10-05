package io.eels.component.csv

import java.io.{OutputStream, Writer}

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings, CsvWriter, CsvWriterSettings}

object CsvSupport {

  def createParser(format: CsvFormat,
                   ignoreLeadingWhitespaces: Boolean = true,
                   ignoreTrailingWhitespaces: Boolean = true,
                   skipEmptyLines: Boolean = true,
                   emptyCellValue: String = null,
                   nullValue: String = null,
                   skipRows: Option[Long] = None,
                   selectedColumns: Seq[String] = Seq.empty): CsvParser = {
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
    settings.setEmptyValue(emptyCellValue)
    settings.setNullValue(nullValue)
    settings.setMaxCharsPerColumn(-1)
    settings.setMaxColumns(2048)
    settings.setReadInputOnSeparateThread(false)
    skipRows.foreach(settings.setNumberOfRowsToSkip)
    selectedColumns.headOption.foreach(_ => settings.selectFields(selectedColumns: _*))
    new com.univocity.parsers.csv.CsvParser(settings)
  }

  def writerSettings(format: CsvFormat,
                     ignoreLeadingWhitespaces: Boolean,
                     ignoreTrailingWhitespaces: Boolean): CsvWriterSettings = {
    val settings = new CsvWriterSettings()
    settings.getFormat.setDelimiter(format.delimiter)
    settings.getFormat.setQuote(format.quoteChar)
    settings.getFormat.setQuoteEscape(format.quoteEscape)
    // we will handle header writing ourselves
    settings.setHeaderWritingEnabled(false)
    settings.setIgnoreLeadingWhitespaces(ignoreLeadingWhitespaces)
    settings.setIgnoreTrailingWhitespaces(ignoreTrailingWhitespaces)
    settings
  }

  def createWriter(writer: Writer,
                   format: CsvFormat,
                   ignoreLeadingWhitespaces: Boolean,
                   ignoreTrailingWhitespaces: Boolean): CsvWriter = {
    new CsvWriter(writer, writerSettings(format, ignoreLeadingWhitespaces, ignoreTrailingWhitespaces))
  }

  def createWriter(output: OutputStream,
                   format: CsvFormat,
                   ignoreLeadingWhitespaces: Boolean,
                   ignoreTrailingWhitespaces: Boolean): CsvWriter = {
    new CsvWriter(output, writerSettings(format, ignoreLeadingWhitespaces, ignoreTrailingWhitespaces))
  }
}
