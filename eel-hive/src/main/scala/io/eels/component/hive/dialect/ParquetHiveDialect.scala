package io.eels.component.hive.dialect

import java.util.function.Consumer

import com.sksamuel.exts.Logging
import io.eels.Row
import io.eels.component.avro.{AvroSchemaFns, RecordSerializer}
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.component.parquet._
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import reactor.core.publisher.{Flux, FluxSink}

import scala.util.control.NonFatal

object ParquetHiveDialect extends HiveDialect with Logging {

  override def read(path: Path,
                    metastoreSchema: StructType,
                    projectionSchema: StructType,
                    predicate: Option[Predicate])
                   (implicit fs: FileSystem, conf: Configuration): Flux[Row] = {

    val schema = AvroSchemaFns.toAvroSchema(projectionSchema)

    Flux.create(new Consumer[FluxSink[Row]] {
      override def accept(sink: FluxSink[Row]): Unit = {
        val reader = ParquetReaderFn(path, predicate, Option(schema))
        try {
          val iter = ParquetRowIterator(reader)
          while (!sink.isCancelled && iter.hasNext) {
            logger.debug("Reading from parquet in thread" + Thread.currentThread().getId)
            sink.next(iter.next)
          }
          sink.complete()
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

  override def writer(schema: StructType,
                      path: Path,
                      permission: Option[FsPermission])
                     (implicit fs: FileSystem, conf: Configuration): HiveWriter = new HiveWriter {
      ParquetLogMute()

    // hive is case insensitive so we must lower case the fields to keep it consistent
    val avroSchema = AvroSchemaFns.toAvroSchema(schema, caseSensitive = false)
    val writer = new ParquetRowWriter(path, avroSchema)
    val serializer = new RecordSerializer(avroSchema)

    override def write(row: Row) {
      require(row.values.nonEmpty, "Attempting to write an empty row")
      val record = serializer.serialize(row)
      writer.write(record)
    }

    override def close() = {
      writer.close()
      permission.foreach(fs.setPermission(path, _))
    }
  }
}