package io.eels.component.sequence

import java.io.StringReader

import com.github.tototoshi.csv.CSVReader
import com.sksamuel.scalax.io.Using
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{InternalRow, Column, Schema, Reader, Source}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable, SequenceFile}

case class SequenceSource(path: Path) extends Source with Using with StrictLogging {
  logger.debug(s"Creating sequence source from $path")

  private def createReader: SequenceFile.Reader = {
    new SequenceFile.Reader(new Configuration, SequenceFile.Reader.file(path))
  }

  private def toValues(v: BytesWritable): Seq[String] = toValues(new String(v.copyBytes, "UTF8"))
  private def toValues(str: String): Seq[String] = {
    val csv = CSVReader.open(new StringReader(str))
    val row = csv.readNext().get
    csv.close()
    row
  }

  override def schema: Schema = {
    logger.debug(s"Fetching sequence schema for $path")
    using(createReader) { reader =>
      val k = new IntWritable
      val v = new BytesWritable
      val columns: List[Column] = {
        reader.next(k, v)
        toValues(v).map(Column.apply)
      }.toList
      Schema(columns)
    }
  }

  override def readers: Seq[Reader] = {
    logger.debug(s"Fetching readers for $path")

    val k = new IntWritable
    val v = new BytesWritable

    val reader = new Reader {

      val reader = createReader

      val columns: Seq[Column] = {
        reader.next(k, v)
        toValues(v).map(Column.apply)
      }

      logger.debug(s"Readers will use schema $columns")

      override def close(): Unit = reader.close()

      override def iterator: Iterator[InternalRow] = new Iterator[InternalRow] {
        override def hasNext: Boolean = reader.next(k, v)
        override def next(): InternalRow = toValues(v)
      }
    }

    Seq(reader)
  }
}
