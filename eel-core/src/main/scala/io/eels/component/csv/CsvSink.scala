package io.eels.component.csv

import java.nio.file.Path

import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import io.eels.{InternalRow, Schema, Sink, SinkWriter}

case class CsvSink(path: Path,
                   writeHeaders: Boolean = true,
                   format: CsvFormat = CsvFormat(),
                   ignoreLeadingWhitespaces: Boolean = false,
                   ignoreTrailingWhitespaces: Boolean = false) extends Sink {
  override def writer(schema: Schema): SinkWriter = new CsvSinkWriter(schema, path, writeHeaders, format, ignoreLeadingWhitespaces, ignoreTrailingWhitespaces)
}

class CsvSinkWriter(schema: Schema,
                    path: Path,
                    writeHeaders: Boolean = true,
                    format: CsvFormat,
                    ignoreLeadingWhitespaces: Boolean = false,
                    ignoreTrailingWhitespaces: Boolean = false) extends SinkWriter {

  val lock = new Object {}

  private def createWriter: CsvWriter = {
    val settings = new CsvWriterSettings()
    settings.getFormat.setDelimiter(format.delimiter)
    settings.getFormat.setQuote(format.quoteChar)
    settings.getFormat.setQuoteEscape(format.quoteEscape)
    settings.setHeaderWritingEnabled(writeHeaders)
    settings.setIgnoreLeadingWhitespaces(ignoreLeadingWhitespaces)
    settings.setIgnoreTrailingWhitespaces(ignoreTrailingWhitespaces)
    new CsvWriter(path.toFile, settings)
  }

  private val writer = createWriter

  override def close(): Unit = writer.close()

  override def write(row: InternalRow): Unit = {
    lock.synchronized {
      val array: Array[String] = row.map(any => if (any == null) null else any.toString).toArray
      writer.writeRow(array)
    }
  }
}