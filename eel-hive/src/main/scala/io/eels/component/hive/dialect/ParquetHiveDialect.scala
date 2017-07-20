package io.eels.component.hive.dialect

import java.util.concurrent.atomic.AtomicInteger

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import com.sksamuel.exts.io.Using
import io.eels.component.hive.{HiveDialect, HiveOps, HiveOutputStream, Publisher}
import io.eels.component.parquet._
import io.eels.component.parquet.util.{ParquetIterator, ParquetLogMute}
import io.eels.datastream.{Cancellable, Subscriber}
import io.eels.schema.StructType
import io.eels.{Predicate, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader

import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.RoundingMode.RoundingMode

class ParquetHiveDialect extends HiveDialect with Logging with Using {

  override def input(path: Path,
                     ignore: StructType,
                     projectionSchema: StructType,
                     predicate: Option[Predicate])
                    (implicit fs: FileSystem, conf: Configuration): Publisher[Seq[Row]] = new Publisher[Seq[Row]] {

    val client = new HiveMetaStoreClient(new HiveConf)
    val ops = new HiveOps(client)

    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
      // convert the eel projection schema into a parquet schema which will be used by the native parquet reader
      try {
        val parquetProjectionSchema = ParquetSchemaFns.toParquetMessageType(projectionSchema)
        using(RowParquetReaderFn(path, predicate, parquetProjectionSchema.some, true)) { reader =>
          val cancellable = new Cancellable {
            override def cancel(): Unit = reader.close()
          }
          subscriber.starting(cancellable)
          val iterator = ParquetIterator(reader)
          val buffer = new ArrayBuffer[Row](1000)
          while (iterator.hasNext) {
            buffer.append(iterator.next)
            if (buffer.size == 1000) {
              subscriber.next(buffer.toVector)
              buffer.clear()
            }
          }
          if (buffer.nonEmpty)
            subscriber.next(buffer.toVector)
          subscriber.completed()
        }
      } catch {
        case t: Throwable => subscriber.error(t)
      }
    }
  }

  override def output(schema: StructType,
                      path: Path,
                      permission: Option[FsPermission],
                      roundingMode: RoundingMode,
                      metadata: Map[String, String])
                     (implicit fs: FileSystem, conf: Configuration): HiveOutputStream = {
    val path_x = path
    new HiveOutputStream {
      ParquetLogMute()

      private val _records = new AtomicInteger(0)
      logger.debug(s"Creating parquet writer at $path")
      private val writer = RowParquetWriterFn(path, schema, metadata, true, roundingMode)

      override def write(row: Row) {
        require(row.values.nonEmpty, "Attempting to write an empty row")
        writer.write(row)
        _records.incrementAndGet()
      }

      override def close(): Unit = {
        logger.debug(s"Closing hive parquet writer $path")
        writer.close()
        // after the files are closed, we should set permissions if we've been asked to, this allows
        // all the files we create to stay consistent
        permission.foreach(fs.setPermission(path, _))
      }

      override def records: Int = _records.get()
      override def path: Path = path_x
    }
  }

  override def stats(path: Path)(implicit fs: FileSystem): Long = {
    import scala.collection.JavaConverters._
    val blocks = ParquetFileReader.readFooter(fs.getConf, path, ParquetMetadataConverter.NO_FILTER).getBlocks.asScala
    blocks.map(_.getRowCount).sum
  }
}