package com.sksamuel.hs

import java.nio.file.Path

import com.github.tototoshi.csv.CSVReader

case class CsvSource(path: Path) extends Source {
  override def loader: Iterator[Seq[String]] = new Iterator[Seq[String]] {
    val reader = CSVReader.open(path.toFile)
    var _next: Option[List[String]] = None
    override def hasNext: Boolean = {
      _next = reader.readNext()
      _next.fold {
        reader.close
        false
      } { _ =>
        true
      }
    }
    override def next(): Seq[String] = _next.get
  }
}
