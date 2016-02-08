package io.eels.component.sequence

import java.io.StringReader

import com.github.tototoshi.csv.CSVReader
import com.sksamuel.scalax.io.Using
import io.eels.{FrameSchema, Column, Field, Part, Row, Source}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable, SequenceFile}

case class SequenceSource(path: Path) extends Source with Using {

  def createReader: SequenceFile.Reader = new SequenceFile.Reader(new Configuration, SequenceFile.Reader.file(path))

  private def toValues(v: BytesWritable): Seq[String] = toValues(new String(v.copyBytes, "UTF8"))
  private def toValues(str: String): Seq[String] = {
    val csv = CSVReader.open(new StringReader(str))
    val row = csv.readNext().get
    csv.close()
    row
  }

  override def schema: FrameSchema = {
    using(createReader) { reader =>
      val k = new IntWritable
      val v = new BytesWritable
      val columns: List[Column] = {
        reader.next(k, v)
        toValues(v).map(Column.apply)
      }.toList
      FrameSchema(columns)
    }
  }

  override def parts: Seq[Part] = {

    val part = new Part {

      val reader = createReader

      val k = new IntWritable
      val v = new BytesWritable

      val columns: Seq[Column] = {
        reader.next(k, v)
        toValues(v).map(Column.apply)
      }

      def iterator: Iterator[Row] = new Iterator[Row] {
        override def hasNext: Boolean = reader.next(k, v)
        override def next(): Row = {
          val fields = toValues(v).map(Field.apply)
          Row(columns.toList, fields.toList)
        }
      }
    }
    Seq(part)
  }
}
