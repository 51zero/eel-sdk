package io.eels.component.sequence

import com.univocity.parsers.csv.CsvWriter
import com.univocity.parsers.csv.CsvWriterSettings
import io.eels.Row
import io.eels.schema.Schema
import io.eels.Sink
import io.eels.SinkWriter
import java.io.StringWriter

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.SequenceFile

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

class SequenceSink(val path: Path) extends Sink {
  override def writer(schema: Schema): SinkWriter = new SequenceSinkWriter(schema, path)

  class SequenceSinkWriter(schema: Schema, path: Path) extends SinkWriter {

    val writer = SequenceFile.createWriter(new Configuration(),
        SequenceFile.Writer.file(path),
      SequenceFile.Writer.keyClass(classOf[IntWritable]),
      SequenceFile.Writer.valueClass(classOf[BytesWritable])
    )

    val key = new IntWritable(0)

    val headers = valuesToCsv(schema.fieldNames())
    writer.append(key, new BytesWritable(headers.getBytes))

    override def close(): Unit = writer.close()

    override def write(row: Row): Unit = {
      this.synchronized {
        val csv = valuesToCsv(row.values)
        writer.append(key, new BytesWritable(csv.getBytes()))
        key.set(key.get() + 1)
      }
    }

    private def valuesToCsv(values: Seq[Any]): String = {
      val swriter = new StringWriter()
      val csv = new CsvWriter(swriter, new CsvWriterSettings())
      csv.writeRow(values)
      csv.close()
      swriter.toString().trim()
    }
  }
}