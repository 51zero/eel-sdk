package io.eels.component.parquet

import com.sksamuel.scalax.io.Using
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.avro.{AvroRecordFn, AvroSchemaFn, AvroSchemaMerge}
import io.eels.{FilePattern, FrameSchema, InternalRow, Reader, Source}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader

case class ParquetSource(pattern: FilePattern) extends Source with StrictLogging with Using {

  private def createReader(path: Path): ParquetReader[GenericRecord] = {
    AvroParquetReader.builder[GenericRecord](path).build().asInstanceOf[ParquetReader[GenericRecord]]
  }

  override def schema: FrameSchema = {
    val paths = pattern.toPaths
    val schemas = paths.map { path =>
      using(createReader(path)) { reader =>
        reader.read.getSchema
      }
    }
    val avroSchema = AvroSchemaMerge("dummy", "com.dummy", schemas)
    AvroSchemaFn.fromAvro(avroSchema)
  }

  override def readers: Seq[Reader] = {

    val paths = pattern.toPaths
    logger.debug(s"Parquet source will read from $paths")

    paths.map { path =>
      new Reader {

        var reader: ParquetReader[GenericRecord] = null

        override def iterator: Iterator[InternalRow] = {
          reader = AvroParquetReader.builder[GenericRecord](path).build().asInstanceOf[ParquetReader[GenericRecord]]
          Iterator.continually(reader.read).takeWhile(_ != null).map(AvroRecordFn.fromRecord)
        }

        override def close(): Unit = if (reader != null) reader.close()
      }
    }
  }
}