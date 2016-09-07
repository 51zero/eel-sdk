package io.eels.component.csv

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}

object CsvSupport {
  def createParser(format: CsvFormat,
                   ignoreLeadingWhitespaces: Boolean = true,
                   ignoreTrailingWhitespaces: Boolean = true,
                   skipEmptyLines: Boolean = true,
                   emptyCellValue: String = null,
                   nullValue: String = null): CsvParser = {
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
    new com.univocity.parsers.csv.CsvParser(settings)
  }
}
