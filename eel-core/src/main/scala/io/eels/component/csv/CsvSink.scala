package io.eels.component.csv

import java.nio.file.Path

import com.github.tototoshi.csv.{CSVFormat, CSVWriter, QUOTE_MINIMAL, Quoting}
import io.eels.{InternalRow, Schema, Sink, SinkWriter}

case class CsvSink(path: Path, props: CsvSinkProps = CsvSinkProps()) extends Sink {
  override def writer(schema: Schema): SinkWriter = new CsvSinkWriter(schema, path, props)
}

class CsvSinkWriter(schema: Schema, path: Path, props: CsvSinkProps) extends SinkWriter {
  self =>

  private val format: CSVFormat = new CSVFormat {
    override val delimiter: Char = props.delimiter
    override val quoteChar: Char = props.quoteChar
    override val treatEmptyLineAsNil: Boolean = false
    override val escapeChar: Char = props.escapeChar
    override val lineTerminator: String = props.lineTerminator
    override val quoting: Quoting = QUOTE_MINIMAL
  }

  private val writer = CSVWriter.open(path.toFile)(format)

  override def close(): Unit = writer.close()

  override def write(row: InternalRow): Unit = {
    self.synchronized {
      writer.writeRow(row)
    }
  }
}

case class CsvSinkProps(delimiter: Char = ',',
                        quoteChar: Char = '"',
                        escapeChar: Char = '"',
                        lineTerminator: String = "\r\n")
