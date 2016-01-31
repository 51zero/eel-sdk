package io.eels.sink

import java.io.StringWriter

import com.github.tototoshi.csv.CSVWriter
import io.eels.{Row, Sink, Writer}
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

    val key = new IntWritable(0)

    override def close(): Unit = writer.close()
    override def write(row: Row): Unit = {
      val swriter = new StringWriter
      val csv = CSVWriter.open(swriter)
      csv.writeRow(row.fields.map(_.value))
      writer.append(key, new BytesWritable(swriter.toString.trim.getBytes))
      csv.close()
      key.set(key.get + 1)
    }
  }
}
