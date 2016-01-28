package io.eels.component.parquet

import io.eels.{Column, Field, Reader, Row, Source}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader

import scala.collection.JavaConverters._

case class ParquetSource(path: Path) extends Source {

  def reader: Reader = new Reader {

    private val reader = new AvroParquetReader[GenericRecord](path)

    private val _iterator = Iterator.continually(reader.read).takeWhile(_ != null).map { record =>
      val columns = record.getSchema.getFields.asScala.map(_.name).map(Column.apply)
      val fields = columns.map(col => record.get(col.name).toString).map(Field.apply)
      Row(columns, fields)
    }

    override val iterator: Iterator[Row] = new Iterator[Row] {
      override def hasNext: Boolean = {
        val hasNext = _iterator.hasNext
        if (!hasNext)
          close()
        hasNext
      }
      override def next(): Row = _iterator.next()
    }

    override def close(): Unit = reader.close()
  }

}