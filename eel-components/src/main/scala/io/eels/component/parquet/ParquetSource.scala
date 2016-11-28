package io.eels.component.parquet

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import com.sksamuel.exts.io.Using
import io.eels.component.avro.{AvroSchemaFns, AvroSchemaMerge}
import io.eels.schema.StructType
import io.eels.{FilePattern, Part, Row, Source}
import io.reactivex.functions.Cancellable
import io.reactivex.{BackpressureStrategy, Flowable, FlowableEmitter, FlowableOnSubscribe}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.{Footer, ParquetFileReader}

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
                        (implicit fs: FileSystem) extends Source with Logging with Using {

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
    logger.debug(s"Parquet source will read from $paths")
    paths.map { it => new ParquetPart(it, predicate) }
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

  override def data(): Flowable[Row] = {

    Flowable.create(new FlowableOnSubscribe[Row] {
      override def subscribe(e: FlowableEmitter[Row]): Unit = {
        val reader = ParquetReaderFn(path, predicate, None)
        try {
          val iter = ParquetRowIterator(reader)
          while (!e.isCancelled && iter.hasNext) {
            logger.debug("Reading from parquet in thread" + Thread.currentThread().getId)
            e.onNext(iter.next)
          }
          e.onComplete()
        } catch {
          case NonFatal(error) =>
            logger.warn("Could not read file", e)
            e.onError(error)
        } finally {
          reader.close()
        }
        e.setCancellable(new Cancellable {
          override def cancel(): Unit = reader.close()
        })
      }
    }, BackpressureStrategy.BUFFER)
  }
}