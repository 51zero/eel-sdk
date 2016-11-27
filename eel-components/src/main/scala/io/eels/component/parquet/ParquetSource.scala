package io.eels.component.parquet

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels.component.avro.{AvroSchemaFns, AvroSchemaMerge}
import io.eels.schema.StructType
import io.eels.{FilePattern, Part, Row, Source}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.{Footer, ParquetFileReader}
import rx.lang.scala.Observable
import scala.collection.JavaConverters._

object ParquetSource {

  def apply(uri: java.net.URI)(implicit fs: FileSystem): ParquetSource =
    apply(FilePattern(new Path(uri.toString)))

  def apply(path: java.nio.file.Path)(implicit fs: FileSystem): ParquetSource =
    apply(FilePattern(path))

  def apply(path: Path)(implicit fs: FileSystem): ParquetSource =
    apply(FilePattern(path))
}

case class ParquetSource(pattern: FilePattern)(implicit fs: FileSystem) extends Source with Logging with Using {

  // the schema returned by the parquet source should be a merged version of the
  // schemas contained in all the files.
  override def schema(): StructType = {
    val paths = pattern.toPaths()
    val schemas = paths.map { path =>
      using(ParquetReaderFn.apply(path, None, None)) { reader =>
        val record = Option(reader.read()).getOrElse {
          sys.error(s"Cannot read $path for schema; file contains no records")
        }
        record.getSchema
      }
    }
    val avroSchema = AvroSchemaMerge("record", "namspace", schemas)
    AvroSchemaFns.fromAvroSchema(avroSchema)
  }

  override def parts(): List[Part] = {
    val paths = pattern.toPaths()
    logger.debug(s"Parquet source will read from $paths")
    paths.map { it => new ParquetPart(it) }
  }

  def footers(): List[Footer] = {
    val paths = pattern.toPaths()
    logger.debug(s"Parquet source will read from $paths")
    paths.flatMap { it =>
      val status = fs.getFileStatus(it)
      logger.debug(s"status=$status; path=$it")
      ParquetFileReader.readAllFootersInParallel(fs.getConf, status).asScala
    }
  }
}

class ParquetPart(path: Path) extends Part {
  override def data(): Observable[Row] = Observable { sub =>
    try {
      sub.onStart()
      val reader = ParquetReaderFn(path, None, None)
      ParquetRowIterator(reader).foreach(sub.onNext)
    } catch {
      case t: Throwable =>
        sub.onError(t)
    } finally {
      if (!sub.isUnsubscribed)
        sub.onCompleted()
    }
  }
}