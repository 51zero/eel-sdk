package io.eels.component.hive.dialect

import java.util.concurrent.atomic.AtomicInteger

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.component.parquet._
import io.eels.component.parquet.util.{ParquetIterator, ParquetLogMute}
import io.eels.schema.StructType
import io.eels.{CloseableIterator, Predicate, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}

object ParquetHiveDialect extends HiveDialect with Logging {

  private val config = ConfigFactory.load()
  private val bufferSize = config.getInt("eel.hive.dialect.reader.buffer-size")

  override def read(path: Path,
                    metastoreSchema: StructType,
                    projectionSchema: StructType,
                    predicate: Option[Predicate])
                   (implicit fs: FileSystem, conf: Configuration): CloseableIterator[Seq[Row]] =
    new CloseableIterator[Seq[Row]] {

      val parquetProjectionSchema = ParquetSchemaFns.toParquetMessageType(projectionSchema)
      val reader = ParquetReaderFn(path, predicate, Option(parquetProjectionSchema))

      override def close(): Unit = {
        super.close()
        reader.close()
      }

      override val iterator: Iterator[Seq[Row]] =
        ParquetIterator(reader).grouped(bufferSize).withPartial(true)
    }

  override def writer(schema: StructType,
                      path: Path,
                      permission: Option[FsPermission],
                      metadata: Map[String, String])
                     (implicit fs: FileSystem, conf: Configuration): HiveWriter = new HiveWriter {
    ParquetLogMute()

    private val _records = new AtomicInteger(0)
    private val writer = ParquetRowWriterFn(path, schema, metadata)

    override def write(row: Row) {
      require(row.values.nonEmpty, "Attempting to write an empty row")
      writer.write(row)
      _records.incrementAndGet()
    }

    override def close() = {
      writer.close()
      // after the files are closed, we should set permissions if we've been asked to, this allows
      // all the files we create to stay consistent
      permission.foreach(fs.setPermission(path, _))
    }

    override def records: Int = _records.get()
  }
}