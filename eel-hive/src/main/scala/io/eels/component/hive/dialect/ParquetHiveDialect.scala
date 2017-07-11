package io.eels.component.hive.dialect

import java.util.concurrent.atomic.AtomicInteger

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import io.eels.component.hive.{HiveDialect, HiveOps, HiveOutputStream, Publisher}
import io.eels.component.parquet._
import io.eels.component.parquet.util.{ParquetIterator, ParquetLogMute}
import io.eels.datastream.Subscriber
import io.eels.schema.StructType
import io.eels.{Predicate, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

class ParquetHiveDialect extends HiveDialect with Logging {

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
        val reader = RowParquetReaderFn(path, predicate, parquetProjectionSchema.some, true)
        val iterator = ParquetIterator(reader)
        iterator.grouped(1000).foreach(subscriber.next)
        subscriber.completed()
      } catch {
        case t: Throwable => subscriber.error(t)
      }
    }
  }

  override def output(schema: StructType,
                      path: Path,
                      permission: Option[FsPermission],
                      metadata: Map[String, String])
                     (implicit fs: FileSystem, conf: Configuration): HiveOutputStream = {
    val path_x = path
    new HiveOutputStream {
      ParquetLogMute()

      private val _records = new AtomicInteger(0)
      logger.debug(s"Creating parquet writer at $path")
      private val writer = RowParquetWriterFn(path, schema, metadata, true)

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
}