package io.eels.component.parquet

import com.sksamuel.scalax.Logging
import com.sksamuel.scalax.io.Using
import io.eels._
import io.eels.component.avro.{AvroSchemaFn, AvroSchemaMerge}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.ParquetReader

case class ParquetSource(pattern: FilePattern) extends Source with Logging with Using {

  private def createReader(path: Path): ParquetReader[GenericRecord] = {
    AvroParquetReader.builder[GenericRecord](path).build().asInstanceOf[ParquetReader[GenericRecord]]
  }

  override def schema: Schema = {
    val paths = pattern.toPaths
    val schemas = paths.map { path =>
      using(createReader(path)) { reader =>
        Option(reader.read).getOrElse(sys.error(s"Cannot read $path for schema; file contains no records")).getSchema
      }
    }
    val avroSchema = AvroSchemaMerge("record", "namspace", schemas)
    AvroSchemaFn.fromAvro(avroSchema)
  }

  override def parts: Seq[Part] = {
    val paths = pattern.toPaths
    logger.debug(s"Parquet source will read from $paths")
    paths.map(new ParquetPart(_))
  }
}

class ParquetPart(path: Path) extends Part {
  override def reader: SourceReader = new ParquetSourceReader(path)
}

class ParquetSourceReader(path: Path) extends SourceReader {
  val reader = ParquetReaderSupport.createReader(path, Nil, null)
  override def iterator: Iterator[InternalRow] = ParquetIterator(reader, Nil)
  override def close(): Unit = reader.close()
}