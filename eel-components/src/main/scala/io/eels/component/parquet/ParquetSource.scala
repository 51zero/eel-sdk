package io.eels.component.parquet

import java.util.function.Consumer

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import com.sksamuel.exts.io.Using
import io.eels._
import io.eels.component.avro.{AvroSchemaFns, AvroSchemaMerge}
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.{Footer, ParquetFileReader}
import reactor.core.publisher.{Flux, FluxSink}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object ParquetSource {

  def apply(uri: java.net.URI)(implicit fs: FileSystem): ParquetSource =
    apply(FilePattern(new Path(uri.toString)))

  def apply(path: java.nio.file.Path)(implicit fs: FileSystem): ParquetSource =
    apply(FilePattern(path))

  def apply(path: Path)(implicit fs: FileSystem): ParquetSource =
    apply(FilePattern(path))
}

case class ParquetSource(pattern: FilePattern,
                         predicate: Option[Predicate] = None)
                        (implicit fs: FileSystem, conf: Configuration) extends Source with Logging with Using {

  def withPredicate(pred: Predicate): ParquetSource = copy(predicate = pred.some)

  // the schema returned by the parquet source should be a merged version of the
  // schemas contained in all the files.
  override def schema(): StructType = {
    val paths = pattern.toPaths()
    val schemas = paths.map { path =>
      using(ParquetReaderFn.apply(path, predicate, None)) { reader =>
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
    logger.debug(s"Parquet source has ${paths.size} files: $paths")
    paths.map { it => new ParquetPart(it, predicate) }
  }

  // returns the count of all records in this source, predicate is ignored
  def countNoPredicate(): Long = {
    val paths = pattern.toPaths()
    if (paths.isEmpty) 0
    else {
      paths.map { path => ParquetFileReader.open(conf, path).getRecordCount }.sum
    }
  }

  def parts2(): List[Part2] = {
    val paths = pattern.toPaths()
    logger.debug(s"Parquet source has ${paths.size} files: $paths")
    paths.map { it => new ParquetPart2(it, predicate) }
  }

  def footers(): List[Footer] = {
    val paths = pattern.toPaths()
    logger.debug(s"Parquet source will read footers from $paths")
    paths.flatMap { it =>
      val status = fs.getFileStatus(it)
      logger.debug(s"status=$status; path=$it")
      ParquetFileReader.readAllFootersInParallel(fs.getConf, status).asScala
    }
  }
}

class ParquetPart(path: Path, predicate: Option[Predicate]) extends Part with Logging {

  override def data(): Flux[Row] = Flux.create(new Consumer[FluxSink[Row]] {
    override def accept(sink: FluxSink[Row]): Unit = {
      //  logger.debug("Starting parquet reader on thread " + Thread.currentThread)
      val reader = ParquetReaderFn(path, predicate, None)
      try {
        val iter = ParquetRowIterator(reader)
        while (!sink.isCancelled && iter.hasNext) {
          sink.next(iter.next)
        }
        sink.complete()
        //    logger.debug(s"Parquet reader completed on thread " + Thread.currentThread)
      } catch {
        case NonFatal(error) =>
          logger.warn("Could not read file", error)
          sink.error(error)
      } finally {
        reader.close()
      }
    }
  }, FluxSink.OverflowStrategy.BUFFER)
}

class ParquetPart2(path: Path,
                   predicate: Option[Predicate]) extends Part2 with Logging {

  override def stream(): PartStream = new PartStream {

    val reader = ParquetReaderFn(path, predicate, None)
    val iter = ParquetRowIterator(reader).grouped(1000).withPartial(true)
    var closed = false

    override def next(): List[Row] = iter.next
    override def hasNext(): Boolean = !closed && iter.hasNext

    override def close(): Unit = {
      closed = true
      reader.close()
    }
  }
}