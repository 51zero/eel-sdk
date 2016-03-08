package io.eels.component.csv

case class CsvFormat(delimiter: Char = ',', quoteChar: Char = '"', quoteEscape: Char = '"', lineSeparator: String = "\n")
