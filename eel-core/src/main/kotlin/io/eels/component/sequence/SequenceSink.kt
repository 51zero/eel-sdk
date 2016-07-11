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

class SequenceSink(val path: Path) : Sink {
  override fun writer(schema: Schema): SinkWriter = SequenceSinkWriter(schema, path)
}

class SequenceSinkWriter(schema: Schema, path: Path) : SinkWriter {

  val writer = SequenceFile.createWriter(Configuration(),
    SequenceFile.Writer.file(path),
      SequenceFile.Writer.keyClass(IntWritable::class.java),
      SequenceFile.Writer.valueClass(BytesWritable::class.java)
  )

  val key = IntWritable(0)

  val headers = valuesToCsv(schema.fieldNames()).apply {
    writer.append(key, BytesWritable(this.toByteArray()))
  }

  override fun close(): Unit = writer.close()

  override fun write(row: Row): Unit {
    synchronized(this) {
      val csv = valuesToCsv(row.values)
      writer.append(key, BytesWritable(csv.toByteArray()))
      key.set(key.get() + 1)
    }
  }

  private fun valuesToCsv(values: List<Any?>): String {
    val swriter = StringWriter()
    val csv = CsvWriter(swriter, CsvWriterSettings())
    csv.writeRow(values)
    csv.close()
    return swriter.toString().trim()
  }
}