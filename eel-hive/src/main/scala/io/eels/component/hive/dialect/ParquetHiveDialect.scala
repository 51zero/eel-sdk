package io.eels.component.hive.dialect

import java.util.concurrent.atomic.AtomicInteger

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import io.eels.component.FlowableIterator
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.component.parquet._
import io.eels.component.parquet.util.{ParquetIterator, ParquetLogMute}
import io.eels.schema.StructType
import io.eels.{Predicate, Row}
import io.reactivex.Flowable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}

class ParquetHiveDialect extends HiveDialect with Logging {

  override def read(path: Path,
                    metastoreSchema: StructType,
                    projectionSchema: StructType,
                    predicate: Option[Predicate])
                   (implicit fs: FileSystem, conf: Configuration): Flowable[Row] = {

    // convert the eel projection schema into a parquet schema which will be used by the native parquet reader
    val parquetProjectionSchema = ParquetSchemaFns.toParquetMessageType(projectionSchema)
    FlowableIterator.create {
      val reader = RowParquetReaderFn(path, predicate, parquetProjectionSchema.some)
      val iterator = ParquetIterator(reader)
      (iterator, reader.close)
    }
  }

  override def writer(schema: StructType,
                      path: Path,
                      permission: Option[FsPermission],
                      metadata: Map[String, String])
                     (implicit fs: FileSystem, conf: Configuration): HiveWriter = {
    val path_x = path
    new HiveWriter {
      ParquetLogMute()

      private val _records = new AtomicInteger(0)
      logger.debug(s"Creating parquet writer at $path with schema $schema")
      private val writer = RowParquetWriterFn(path, schema, metadata)

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