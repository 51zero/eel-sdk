package io.eels.component.csv

import com.univocity.parsers.csv.CsvWriter
import io.eels.schema.Schema
import io.eels.{Row, Sink, SinkWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

case class CsvSink(path: Path,
                   headers: Header = Header.FirstRow,
                   format: CsvFormat = CsvFormat(),
                   ignoreLeadingWhitespaces: Boolean = false,
                   ignoreTrailingWhitespaces: Boolean = false)
                  (implicit fs: FileSystem) extends Sink {

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

    import scala.collection.JavaConverters._

    private lazy val writer: CsvWriter = {
      val output = fs.create(path)
      val writer = CsvSupport.createWriter(output, format, ignoreLeadingWhitespaces, ignoreTrailingWhitespaces)
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
          case null => ""
          case other => other.toString
        }
        writer.writeRow(array: _*)
      }
    }
  }
}

object CsvSink {
  def apply(path: java.nio.file.Path): CsvSink = CsvSink(new Path(path.toString))(FileSystem.getLocal(new Configuration))
}