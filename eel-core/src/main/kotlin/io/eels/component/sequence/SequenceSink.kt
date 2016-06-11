package io.eels.component.sequence

import io.eels.Row
import io.eels.Schema
import io.eels.Sink
import io.eels.SinkWriter
import java.io.StringWriter

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.SequenceFile

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

class SequenceSink(val path: Path) : Sink {
  override fun writer(schema: Schema): SinkWriter = SequenceSinkWriter(schema, path)
}

class SequenceSinkWriter(schema: Schema, path: Path) : SinkWriter {

  val writer = SequenceFile.createWriter(Configuration(),
    SequenceFile.Writer.file(path),
      SequenceFile.Writer.keyClass(classOf<IntWritable>),
      SequenceFile.Writer.valueClass(classOf<BytesWritable>)
  )

  val key = IntWritable(0)

  val headers = valuesToCsv(schema.columnNames())
  writer.append(key, BytesWritable(headers.getBytes("UTF8")))

  override fun close(): Unit = writer.close()

  override fun write(row: Row): Unit {
    synchronized(this) {
      val csv = valuesToCsv(row.values)
      writer.append(key, BytesWritable(csv.getBytes("UTF8")))
      key.set(key.get() + 1)
    }
  }

  private fun valuesToCsv(values: List<Any?>): String {
    val swriter = StringWriter()
    val csv = CSVWriter(swriter)
    csv.writeRow(values)
    csv.close()
    return swriter.toString().trim()
  }
}