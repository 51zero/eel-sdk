package io.eels.component.csv

data class CsvFormat(val delimiter: Char = ',', val quoteChar: Char = '"', val quoteEscape: Char = '"', val lineSeparator: String = "\n")
