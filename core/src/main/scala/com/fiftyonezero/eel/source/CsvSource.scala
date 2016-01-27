package com.fiftyonezero.eel.source

import java.nio.file.Path

import com.fiftyonezero.eel.{Reader, Source, FrameSchema, Row, Column, Field}
import com.github.tototoshi.csv.CSVReader

case class CsvSource(path: Path, overrideSchema: Option[FrameSchema] = None) extends Source {

  def withSchema(schema: FrameSchema): CsvSource = copy(overrideSchema = Some(schema))

  override def schema: FrameSchema = overrideSchema.getOrElse(super.schema)

  override def reader: Reader = new Reader {

    private val reader = CSVReader.open(path.toFile)
    private val (_iterator, headers) = {
      val iterator = reader.iterator
      val headers = iterator.next.map(Column.apply)
      (iterator, headers)
    }

    override def close(): Unit = reader.close()

    override def iterator: Iterator[Row] = new Iterator[Row] {
      override def hasNext: Boolean = {
        val hasNext = _iterator.hasNext
        if (!hasNext)
          close()
        hasNext
      }
      override def next: Row = Row(headers, _iterator.next.map(Field.apply))
    }
  }
}
