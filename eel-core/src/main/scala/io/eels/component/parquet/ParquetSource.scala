package io.eels.component.parquet

import com.sksamuel.scalax.Logging
import com.sksamuel.scalax.io.Using
import io.eels._
import io.eels.component.avro.{AvroSchemaFn, AvroSchemaMerge}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.hadoop.{Footer, ParquetFileReader, ParquetReader}

import scala.collection.JavaConverters._

case class ParquetSource(pattern: FilePattern)(implicit fs: FileSystem) extends Source with Logging with Using {

  private def createReader(path: Path): ParquetReader[GenericRecord] = {
    AvroParquetReader.builder[GenericRecord](path).build().asInstanceOf[ParquetReader[GenericRecord]]
  }

  override lazy val schema: Schema = {
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
    paths.map(new ParquetPart(_, schema))
  }

  def footers: Seq[Footer] = {
    val paths = pattern.toPaths
    logger.debug(s"Parquet source will read from $paths")
    paths.flatMap { path =>
      val status = fs.getFileStatus(path)
      logger.debug(s"status=$status; path=$path")
      ParquetFileReader.readAllFootersInParallel(new Configuration, status).asScala
    }
  }
}

class ParquetPart(path: Path, schema: Schema) extends Part {
  override def reader: SourceReader = new ParquetSourceReader(path, schema)
}

class ParquetSourceReader(path: Path, schema: Schema) extends SourceReader {
  val reader = ParquetReaderSupport.createReader(path, false, None, schema)
  override def iterator: Iterator[InternalRow] = ParquetIterator(reader, schema)
  override def close(): Unit = reader.close()
}