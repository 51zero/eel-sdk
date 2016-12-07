package io.eels.component.parquet

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import com.sksamuel.exts.io.Using
import io.eels._
import io.eels.component.avro.{AvroSchemaFns, AvroSchemaMerge}
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.{Footer, ParquetFileReader}

import scala.collection.JavaConverters._

object ParquetSource {

  def apply(uri: java.net.URI)(implicit fs: FileSystem, conf: Configuration): ParquetSource =
    apply(FilePattern(new Path(uri.toString)))

  def apply(path: java.nio.file.Path)(implicit fs: FileSystem, conf: Configuration): ParquetSource =
    apply(FilePattern(path))

  def apply(path: Path)(implicit fs: FileSystem, conf: Configuration): ParquetSource =
    apply(FilePattern(path))
}

case class ParquetSource(pattern: FilePattern,
                         predicate: Option[Predicate] = None)
                        (implicit fs: FileSystem, conf: Configuration) extends Source with Logging with Using {

  lazy val paths = pattern.toPaths()

  def withPredicate(pred: Predicate): ParquetSource = copy(predicate = pred.some)

  // the schema returned by the parquet source should be a merged version of the
  // schemas contained in all the files.
  override def schema(): StructType = {
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

  // returns the count of all records in this source, predicate is ignored
  def countNoPredicate(): Long = statistics().count

  // returns stats, predicate is ignored
  def statistics(): Statistics = {
    if (paths.isEmpty) Statistics.Empty
    else {
      paths.foldLeft(Statistics.Empty) { (stats, path) =>
        val footer = ParquetFileReader.readFooter(conf, path)
        footer.getBlocks.asScala.foldLeft(stats) { (stats, block) =>
          stats.copy(
            count = stats.count + block.getRowCount,
            compressedSize = stats.compressedSize + block.getCompressedSize,
            uncompressedSize = stats.uncompressedSize + block.getTotalByteSize
          )
        }
      }
    }
  }

  override def parts2(): List[Part] = {
    logger.debug(s"Parquet source has ${paths.size} files: $paths")
    paths.map { it => new ParquetPart(it, predicate) }
  }

  def footers(): List[Footer] = {
    logger.debug(s"Parquet source will read footers from $paths")
    paths.flatMap { it =>
      val status = fs.getFileStatus(it)
      logger.debug(s"status=$status; path=$it")
      ParquetFileReader.readAllFootersInParallel(fs.getConf, status).asScala
    }
  }
}

case class Statistics(count: Long, compressedSize: Long, uncompressedSize: Long)

object Statistics {
  val Empty = Statistics(0, 0, 0)
}

class ParquetPart(path: Path,
                  predicate: Option[Predicate]) extends Part with Logging {

  override def iterator(): CloseableIterator[List[Row]] = new CloseableIterator[List[Row]] {

    val reader = ParquetReaderFn(path, predicate, None)
    val iter = ParquetRowIterator(reader).grouped(100).withPartial(true)
    var closed = false

    override def next(): List[Row] = iter.next
    override def hasNext(): Boolean = !closed && iter.hasNext

    override def close(): Unit = {
      closed = true
      reader.close()
    }
  }
}