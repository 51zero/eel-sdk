package com.sksamuel.hs.source

import com.sksamuel.hs.Source
import com.sksamuel.hs.sink.{Field, Column, Row}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import scala.collection.JavaConverters._

case class ParquetSource(path: Path) extends Source {

  override def loader: Iterator[Row] = new Iterator[Row] {

    lazy val reader = new AvroParquetReader[GenericRecord](path)

    lazy val iterator = Iterator.continually(reader.read).takeWhile(_ != null).map { record =>
      val columns = record.getSchema.getFields.asScala.map(_.name).map(Column.apply)
      val fields = columns.map(col => record.get(col.name).toString).map(Field.apply)
      Row(columns, fields)
    }

    override def hasNext: Boolean = iterator.hasNext
    override def next(): Row = iterator.next()
  }
}
