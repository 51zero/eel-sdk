package io.eels.component.hive.dialect

import java.util.concurrent.atomic.AtomicInteger

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import com.sksamuel.exts.io.Using
import io.eels.component.hive.{HiveDialect, HiveOps, HiveOutputStream}
import io.eels.component.parquet._
import io.eels.component.parquet.util.{ParquetIterator, ParquetLogMute}
import io.eels.datastream.{DataStream, Publisher, Subscriber, Subscription}
import io.eels.schema.StructType
import io.eels.{Chunk, Predicate, Rec}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
import org.apache.hadoop.hive.ql.io.parquet.{MapredParquetInputFormat, MapredParquetOutputFormat}

import scala.math.BigDecimal.RoundingMode.RoundingMode

case class ParquetHiveDialect(options: ParquetWriteOptions = ParquetWriteOptions()) extends HiveDialect with Logging with Using {

  override val serde: String = classOf[ParquetHiveSerDe].getCanonicalName
  override val inputFormat: String = classOf[MapredParquetInputFormat].getCanonicalName
  override val outputFormat: String = classOf[MapredParquetOutputFormat].getCanonicalName

  override def input(path: Path,
                     ignore: StructType,
                     projectionSchema: StructType,
                     predicate: Option[Predicate])
                    (implicit fs: FileSystem, conf: Configuration): Publisher[Chunk] = new Publisher[Chunk] {

    val client = new HiveMetaStoreClient(new HiveConf)
    val ops = new HiveOps(client)

    override def subscribe(subscriber: Subscriber[Chunk]): Unit = {
      // convert the eel projection schema into a parquet schema which will be used by the native parquet reader
      try {
        val parquetProjectionSchema = ParquetSchemaFns.toParquetMessageType(projectionSchema)
        using(RecordParquetReaderFn(path, predicate, parquetProjectionSchema.some, true)) { reader =>
          val subscription = new Subscription {
            override def cancel(): Unit = reader.close()
          }
          subscriber.subscribed(subscription)
          ParquetIterator(reader).grouped(DataStream.DefaultBatchSize).foreach(subscriber.next)
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

      override def write(row: Rec) {
        require(row.nonEmpty, "Attempting to write an empty row")
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
}