package io.eels.component.sequence

import java.io.StringWriter

import com.github.tototoshi.csv.CSVWriter
import io.eels.{Schema, InternalRow, Sink, SinkWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable, SequenceFile}

case class SequenceSink(path: Path) extends Sink {
  self =>
  override def writer(schema: Schema): SinkWriter = new SequenceSinkWriter(schema, path)
}

class SequenceSinkWriter(schema: Schema, path: Path) extends SinkWriter {
  self =>

  val writer = SequenceFile.createWriter(new Configuration,
    SequenceFile.Writer.file(path),
    SequenceFile.Writer.keyClass(classOf[IntWritable]),
    SequenceFile.Writer.valueClass(classOf[BytesWritable])
  )

  val key = new IntWritable(0)

  val headers = valuesToCsv(schema.columnNames)
  writer.append(key, new BytesWritable(headers.getBytes("UTF8")))

  override def close(): Unit = writer.close()

  override def write(row: InternalRow): Unit = {
    self.synchronized {
      val csv = valuesToCsv(row)
      writer.append(key, new BytesWritable(csv.getBytes("UTF8")))
      key.set(key.get + 1)
    }
  }

  private def valuesToCsv(values: Seq[Any]): String = {
    val swriter = new StringWriter
    val csv = CSVWriter.open(swriter)
    csv.writeRow(values)
    csv.close()
    swriter.toString.trim
  }
}