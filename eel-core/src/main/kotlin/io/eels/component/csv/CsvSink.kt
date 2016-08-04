package io.eels.component.csv

import com.univocity.parsers.csv.CsvWriter
import com.univocity.parsers.csv.CsvWriterSettings
import io.eels.Row
import io.eels.Sink
import io.eels.SinkWriter
import io.eels.schema.Schema
import java.nio.file.Path

data class CsvSink @JvmOverloads constructor(val path: Path,
                                             val headers: Header = Header.None,
                                             val format: CsvFormat = CsvFormat(),
                                             val ignoreLeadingWhitespaces: Boolean = false,
                                             val ignoreTrailingWhitespaces: Boolean = false) : Sink {

  override fun writer(schema: Schema): SinkWriter = CsvSinkWriter(schema, path, headers, format, ignoreLeadingWhitespaces, ignoreTrailingWhitespaces)

  fun withHeaders(headers: Header): CsvSink = copy(headers = headers)
  fun withIgnoreLeadingWhitespaces(ignoreLeadingWhitespaces: Boolean): CsvSink = copy(ignoreLeadingWhitespaces = ignoreLeadingWhitespaces)
  fun withIgnoreTrailingWhitespaces(ignoreTrailingWhitespaces: Boolean): CsvSink = copy(ignoreTrailingWhitespaces = ignoreTrailingWhitespaces)
  fun withFormat(format: CsvFormat): CsvSink = copy(format = format)

  inner class CsvSinkWriter(val schema: Schema,
                            val path: Path,
                            val headers: Header,
                            val format: CsvFormat,
                            val ignoreLeadingWhitespaces: Boolean = false,
                            val ignoreTrailingWhitespaces: Boolean = false) : SinkWriter {

    private val lock = Any()

    private fun createWriter(): CsvWriter {
      val settings = CsvWriterSettings()
      settings.format.delimiter = format.delimiter
      settings.format.quote = format.quoteChar
      settings.format.quoteEscape = format.quoteEscape
      // we will handle header writing ourselves
      settings.isHeaderWritingEnabled = false
      settings.ignoreLeadingWhitespaces = ignoreLeadingWhitespaces
      settings.ignoreTrailingWhitespaces = ignoreTrailingWhitespaces
      return CsvWriter(path.toFile(), settings)
    }

    private val writer: CsvWriter by lazy {
      val writer = createWriter()
      when (headers) {
        Header.FirstComment -> writer.commentRow(schema.fieldNames().joinToString(format.delimiter.toString()))
        Header.FirstRow -> writer.writeHeaders(schema.fieldNames())
        else -> {
        }
      }
      writer
    }

    override fun close(): Unit = writer.close()

    override fun write(row: Row): Unit {
      synchronized(lock) {
        // nulls should be written as empty strings
        val array = row.values.map { it?.toString() }.toTypedArray()
        writer.writeRow(array)
      }
    }
  }
}