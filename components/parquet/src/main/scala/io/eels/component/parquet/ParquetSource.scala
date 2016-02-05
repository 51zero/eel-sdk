package io.eels.component.parquet

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Column, Field, FilePattern, Reader, Row, Source}
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader

import scala.collection.JavaConverters._

case class ParquetSource(pattern: FilePattern) extends Source with StrictLogging {

  def reader: Reader = new Reader {

    private val paths = pattern.toPaths
    logger.debug(s"Parquet source will read from $paths")
    private val readers = paths.map(AvroParquetReader.builder[GenericRecord](_).build().asInstanceOf[ParquetReader[GenericRecord]])

    private val iterators = readers.map(reader => Iterator.continually(reader.read).takeWhile(_ != null).map { record =>
      val columns = record.getSchema.getFields.asScala.map(_.name).map(Column.apply)
      val fields = columns.map(col => Option(record.get(col.name)).map(_.toString).orNull).map(Field.apply)
      Row(columns, fields)
    })
    private val mergedIterator = iterators.reduce((a, b) => a ++ b)

    override val iterator: Iterator[Row] = new Iterator[Row] {
      override def hasNext: Boolean = {
        val hasNext = mergedIterator.hasNext
        if (!hasNext)
          close()
        hasNext
      }
      override def next(): Row = mergedIterator.next()
    }

    override def close(): Unit = readers.foreach(_.close)
  }
}