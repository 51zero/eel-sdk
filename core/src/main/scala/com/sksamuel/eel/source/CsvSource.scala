package com.sksamuel.eel.source

import java.nio.file.Path

import com.github.tototoshi.csv.CSVReader
import com.sksamuel.eel.Source
import com.sksamuel.eel.sink.{Column, Field, Row}

case class CsvSource(path: Path) extends Source {

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
