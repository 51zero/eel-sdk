package io.eels.component.csv

import java.nio.file.Path

import com.univocity.parsers.csv.CsvWriter
import com.univocity.parsers.csv.CsvWriterSettings
import io.eels.Row
import io.eels.schema.Schema
import io.eels.Sink
import io.eels.SinkWriter

data class CsvSink(val path: Path,
                   val writeHeaders: Boolean = true,
                   val format: CsvFormat = CsvFormat(),
                   val ignoreLeadingWhitespaces: Boolean = false,
                   val ignoreTrailingWhitespaces: Boolean = false) : Sink {
  override fun writer(schema: Schema): SinkWriter = CsvSinkWriter(schema, path, writeHeaders, format, ignoreLeadingWhitespaces, ignoreTrailingWhitespaces)
}

class CsvSinkWriter(val schema: Schema,
                    val path: Path,
                    val writeHeaders: Boolean = true,
                    val format: CsvFormat,
                    val ignoreLeadingWhitespaces: Boolean = false,
                    val ignoreTrailingWhitespaces: Boolean = false) : SinkWriter {

  val lock = Any()

  private fun createWriter(): CsvWriter {
    val settings = CsvWriterSettings()
    settings.format.delimiter = format.delimiter
    settings.format.quote = format.quoteChar
    settings.format.quoteEscape = format.quoteEscape
    settings.isHeaderWritingEnabled = writeHeaders
    settings.ignoreLeadingWhitespaces = ignoreLeadingWhitespaces
    settings.ignoreTrailingWhitespaces = ignoreTrailingWhitespaces
    return CsvWriter(path.toFile(), settings)
  }

  private val writer = createWriter()

  override fun close(): Unit = writer.close()

  override fun write(row: Row): Unit {
    synchronized(lock) {
      val array = row.values.map { it?.toString() }.toTypedArray()
      writer.writeRow(array)
    }
  }
}