package io.eels.component.sequence

import java.io.StringReader

import com.github.tototoshi.csv.CSVReader
import io.eels.{Column, Field, Reader, Row, Source}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable, SequenceFile}

case class SequenceSource(path: Path) extends Source {
  override def reader: Reader = new Reader {

    val reader = new SequenceFile.Reader(new Configuration, SequenceFile.Reader.file(path))

    val k = new IntWritable
    val v = new BytesWritable

    val columns: Seq[Column] = {
      reader.next(k, v)
      toValues(v).map(Column.apply)
    }

    def toValues(b: BytesWritable): Seq[String] = toValues(new String(v.copyBytes, "UTF8"))
    def toValues(str: String): Seq[String] = {
      val csv = CSVReader.open(new StringReader(str))
      val row = csv.readNext().get
      csv.close()
      row
    }

    override def close(): Unit = reader.close()
    override def iterator: Iterator[Row] = new Iterator[Row] {
      override def hasNext: Boolean = reader.next(k, v)
      override def next(): Row = {
        val fields = toValues(v).map(Field.apply)
        Row(columns, fields)
      }
    }
  }
}
