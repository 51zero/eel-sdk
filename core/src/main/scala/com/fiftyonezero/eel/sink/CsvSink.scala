package com.fiftyonezero.eel.sink

import java.nio.file.Path

import com.fiftyonezero.eel.{Writer, Sink, Row}
import com.github.tototoshi.csv.{QUOTE_MINIMAL, Quoting, CSVFormat, CSVWriter}
import com.sksamuel.eel.Writer

case class CsvSink(path: Path, props: CsvSinkProps = CsvSinkProps()) extends Sink {

  override def writer: Writer = new Writer {

    val writer = CSVWriter.open(path.toFile)(format)

    private def format: CSVFormat = new CSVFormat {
      override val delimiter: Char = props.delimiter
      override val quoteChar: Char = props.quoteChar
      override val treatEmptyLineAsNil: Boolean = false
      override val escapeChar: Char = props.escapeChar
      override val lineTerminator: String = props.lineTerminator
      override val quoting: Quoting = QUOTE_MINIMAL
    }

    override def close(): Unit = writer.close

    override def write(row: Row): Unit = {
      writer.writeRow(row.fields.map(_.value))
    }
  }

}

case class CsvSinkProps(delimiter: Char = ',',
                        quoteChar: Char = '"',
                        escapeChar: Char = '"',
                        lineTerminator: String = "\r\n")
