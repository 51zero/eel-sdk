package io.eels.component.parquet

import com.sksamuel.scalax.io.Using
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.avro.AvroRecordFn
import io.eels.{FilePattern, FrameSchema, Reader, Row, Source}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader

import scala.collection.JavaConverters._

case class ParquetSource(pattern: FilePattern) extends Source with StrictLogging with Using {

  override def schema: FrameSchema = {
    val path = pattern.toPaths.head
    using(ParquetIterator.createReader(path)) { reader =>
      val record = reader.read()
      val columns = record.getSchema.getFields.asScala.map(_.name)
      FrameSchema(columns)
    }
  }

  override def readers: Seq[Reader] = {

    val paths = pattern.toPaths
    logger.debug(s"Parquet source will read from $paths")

    paths.map { path =>
      new Reader {

        var reader: ParquetReader[GenericRecord] = null

        override def iterator: Iterator[Row] = {
          reader = AvroParquetReader.builder[GenericRecord](path).build().asInstanceOf[ParquetReader[GenericRecord]]
          Iterator.continually(reader.read).takeWhile(_ != null).map(AvroRecordFn.fromRecord)
        }

        override def close(): Unit = if (reader != null) reader.close()
      }
    }
  }
}

object ParquetIterator extends StrictLogging {

  def createReader(path: Path): ParquetReader[GenericRecord] = {
    AvroParquetReader.builder[GenericRecord](path).build().asInstanceOf[ParquetReader[GenericRecord]]
  }

  def apply(path: Path): Iterator[Row] = new Iterator[Row] {

    val reader = createReader(path)
    val iter = Iterator.continually(reader.read).takeWhile(_ != null).map(AvroRecordFn.fromRecord)

    override def hasNext: Boolean = {
      val hasNext = iter.hasNext
      if (!hasNext) {
        logger.debug("Closing parquet iterator")
        reader.close()
      }
      hasNext
    }
    override def next(): Row = iter.next()
  }
}