package io.eels.component.csv

import java.nio.file.Path

import com.github.tototoshi.csv.CSVReader
import com.sksamuel.scalax.io.Using
import io.eels.{Field, FrameSchema, Part, Row, Source}

case class CsvSource(path: Path, overrideSchema: Option[FrameSchema] = None) extends Source with Using {

  def withSchema(schema: FrameSchema): CsvSource = copy(overrideSchema = Some(schema))

  override def schema: FrameSchema = overrideSchema.getOrElse {
    val reader = CSVReader.open(path.toFile)
    using(reader) { reader =>
      val headers = reader.readNext().get
      FrameSchema(headers)
    }
  }

  override def parts: Seq[Part] = {

    val part = new Part {

      override def iterator: Iterator[Row] = new Iterator[Row] {

        val reader = CSVReader.open(path.toFile)
        val iter = reader.iterator
        val schema = FrameSchema(iter.next)

        override def hasNext: Boolean = {
          val hasNext = iter.hasNext
          if (!hasNext)
            reader.close()
          hasNext
        }

        override def next: Row = Row(schema.columns, iter.next.map(Field.apply).toList)
      }
    }

    Seq(part)
  }
}
