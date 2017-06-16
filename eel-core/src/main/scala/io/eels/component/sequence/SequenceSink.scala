package io.eels.component.sequence

import java.io.StringWriter

import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import io.eels.{Row, Sink, RowOutputStream}
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable, SequenceFile}

case class SequenceSink(path: Path)(implicit conf: Configuration) extends Sink {

  override def open(schema: StructType): RowOutputStream = new SequenceRowOutputStream(schema, path)

  class SequenceRowOutputStream(schema: StructType, path: Path) extends RowOutputStream {

    val writer = SequenceFile.createWriter(conf,
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
      csv.writeRow(values.map {
        case null => null
        case other => other.toString
      }: _*)
      csv.close()
      swriter.toString().trim()
    }
  }
}