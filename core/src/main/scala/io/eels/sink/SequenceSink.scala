package io.eels.sink

import java.io.StringWriter

import com.github.tototoshi.csv.CSVWriter
import io.eels.{Column, Row, Sink, Writer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable, SequenceFile}

case class SequenceSink(path: Path) extends Sink {

  override def writer: Writer = new Writer {

    val writer = SequenceFile.createWriter(new Configuration,
      SequenceFile.Writer.file(path),
      SequenceFile.Writer.keyClass(classOf[IntWritable]),
      SequenceFile.Writer.valueClass(classOf[BytesWritable])
    )

    var _writtenHeader = false
    val key = new IntWritable(0)

    def writeHeader(columns: Seq[Column]): Unit = {
      val csv = valuesToCsv(columns.map(_.name))
      writer.append(key, new BytesWritable(csv.getBytes("UTF8")))
      _writtenHeader = true
    }

    def valuesToCsv(values: Seq[String]): String = {
      val swriter = new StringWriter
      val csv = CSVWriter.open(swriter)
      csv.writeRow(values)
      csv.close()
      swriter.toString.trim
    }

    override def close(): Unit = writer.close()
    override def write(row: Row): Unit = {
      if (!_writtenHeader) writeHeader(row.columns)
      val csv = valuesToCsv(row.fields.map(_.value))
      writer.append(key, new BytesWritable(csv.getBytes("UTF8")))
      key.set(key.get + 1)
    }
  }
}
