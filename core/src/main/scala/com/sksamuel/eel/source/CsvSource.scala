package com.sksamuel.eel.source

import java.nio.file.Path

import com.github.tototoshi.csv.CSVReader
import com.sksamuel.eel.{FrameSchema, Field, Column, Row, Source}

case class CsvSource(path: Path, overrideSchema: Option[FrameSchema] = None) extends Source {

  override def schema: FrameSchema = overrideSchema.getOrElse(super.schema)

  def withSchema(schema: FrameSchema): CsvSource = copy(overrideSchema = Some(schema))

  private def iterator: Iterator[Row] = new Iterator[Row] {
    val reader = CSVReader.open(path.toFile)
    lazy val (iterator, headers) = {
      val iterator = reader.iterator
      val headers = iterator.next.map(Column.apply)
      (iterator, headers)
    }
    override def hasNext: Boolean = iterator.hasNext
    override def next(): Row = Row(headers, iterator.next.map(Field.apply))
  }

  override def loader: Iterator[Row] = iterator
}
