package io.eels.component.parquet

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import com.sksamuel.exts.io.Using
import io.eels._
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.format.converter.ParquetMetadataConverter
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
                         predicate: Option[Predicate] = None,
                         projection: Seq[String] = Nil)
                        (implicit fs: FileSystem, conf: Configuration) extends Source with Logging with Using {

  lazy val paths = pattern.toPaths()

  def withPredicate(pred: Predicate): ParquetSource = copy(predicate = pred.some)
  def withProjection(first: String, rest: String*): ParquetSource = withProjection(first +: rest)
  def withProjection(fields: Seq[String]): ParquetSource = {
    require(fields.nonEmpty)
    copy(projection = fields.toList)
  }

  // returns the metadata in the parquet file, or an empty map if none
  def metadata(): Map[String, String] = {
    paths.foldLeft(Map.empty[String, String]) { (metadata, path) =>
      val footer = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER)
      metadata ++ footer.getFileMetaData.getKeyValueMetaData.asScala
    }
  }

  lazy val schema: StructType = {
    using(ParquetReaderFn(paths.head, None, None)) { reader =>
      val row = Option(reader.read).getOrElse {
        sys.error(s"Cannot read ${paths.head} for schema; file contains no records")
      }
      row.schema
    }
  }

  // returns the count of all records in this source, predicate is ignored
  def countNoPredicate(): Long = statistics().count

  // returns stats, predicate is ignored
  def statistics(): Statistics = {
    if (paths.isEmpty) Statistics.Empty
    else {
      paths.foldLeft(Statistics.Empty) { (stats, path) =>
        val footer = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER)
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

  override def parts(): List[Part] = {
    logger.debug(s"Parquet source has ${paths.size} files: $paths")
    paths.map { it => new ParquetPart(it, predicate, projection) }
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