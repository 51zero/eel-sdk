package io.eels.component.csv

import java.nio.file.Path

import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import io.eels.{Row, Sink, SinkWriter}
import io.eels.schema.Schema

case class CsvSink(path: Path,
                   headers: Header = Header.None,
                   format: CsvFormat = CsvFormat(),
                   ignoreLeadingWhitespaces: Boolean = false,
                   ignoreTrailingWhitespaces: Boolean = false) extends Sink {

  override def writer(schema: Schema): SinkWriter = new CsvSinkWriter(schema, path, headers, format, ignoreLeadingWhitespaces, ignoreTrailingWhitespaces)

  def withHeaders(headers: Header): CsvSink = copy(headers = headers)
  def withIgnoreLeadingWhitespaces(ignoreLeadingWhitespaces: Boolean): CsvSink = copy(ignoreLeadingWhitespaces = ignoreLeadingWhitespaces)
  def withIgnoreTrailingWhitespaces(ignoreTrailingWhitespaces: Boolean): CsvSink = copy(ignoreTrailingWhitespaces = ignoreTrailingWhitespaces)
  def withFormat(format: CsvFormat): CsvSink = copy(format = format)

  class CsvSinkWriter(val schema: Schema,
                      val path: Path,
                      val headers: Header,
                      val format: CsvFormat,
                      val ignoreLeadingWhitespaces: Boolean = false,
                      val ignoreTrailingWhitespaces: Boolean = false) extends SinkWriter {

    private val lock = new AnyRef {}

    private def createWriter(): CsvWriter = {
      val settings = new CsvWriterSettings()
      settings.getFormat.setDelimiter(format.delimiter)
      settings.getFormat.setQuote(format.quoteChar)
      settings.getFormat.setQuoteEscape(format.quoteEscape)
      // we will handle header writing ourselves
      settings.setHeaderWritingEnabled(false)
      settings.setIgnoreLeadingWhitespaces(ignoreLeadingWhitespaces)
      settings.setIgnoreTrailingWhitespaces(ignoreTrailingWhitespaces)
      new CsvWriter(path.toFile(), settings)
    }

    import scala.collection.JavaConverters._

    private lazy val writer: CsvWriter = {
      val writer = createWriter()
      headers match {
        case Header.FirstComment => writer.commentRow(schema.fieldNames().mkString(format.delimiter.toString()))
        case Header.FirstRow => writer.writeHeaders(schema.fieldNames().asJava)
        case _ =>
      }
      writer
    }

    override def close(): Unit = writer.close()

    override def write(row: Row): Unit = {
      lock.synchronized {
        // nulls should be written as empty strings
        val array = row.values.map {
          case null => null
          case other => other.toString
        }.toArray
        writer.writeRow(array)
      }
    }
  }
}