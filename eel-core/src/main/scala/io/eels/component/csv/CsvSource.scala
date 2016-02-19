package io.eels.component.csv

import java.nio.file.Path

import com.github.tototoshi.csv.CSVReader
import com.sksamuel.scalax.io.Using
import io.eels.{FrameSchema, Reader, Row, Source}

case class CsvSource(path: Path, overrideSchema: Option[FrameSchema] = None) extends Source with Using {

  def withSchema(schema: FrameSchema): CsvSource = copy(overrideSchema = Some(schema))

  override def schema: FrameSchema = overrideSchema.getOrElse {
    val reader = CSVReader.open(path.toFile)
    using(reader) { reader =>
      val headers = reader.readNext().get
      FrameSchema(headers)
    }
  }

  override def readers: Seq[Reader] = {

    val reader = CSVReader.open(path.toFile)
    val iter = reader.iterator

    // throw away header
    if (iter.hasNext)
      iter.next

    val part = new Reader {

      override def close(): Unit = reader.close()

      override def iterator: Iterator[Row] = new Iterator[Row] {

        override def hasNext: Boolean = {
          val hasNext = iter.hasNext
          if (!hasNext)
            reader.close()
          hasNext
        }

        override def next: Row = iter.next
      }
    }

    Seq(part)
  }
}
