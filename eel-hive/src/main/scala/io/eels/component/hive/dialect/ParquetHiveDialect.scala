package io.eels.component.hive.dialect

import com.sksamuel.exts.Logging
import io.eels.component.avro.{AvroRecordSerializer, AvroSchemaFns, RecordSerializer}
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.component.parquet._
import io.eels.schema.Schema
import io.eels.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import rx.lang.scala.Observable

import scala.util.control.NonFatal

object ParquetHiveDialect extends HiveDialect with Logging {

  override def read(path: Path,
                    metastoreSchema: Schema,
                    projectionSchema: Schema,
                    predicate: Option[Predicate])
                   (implicit fs: FileSystem, conf: Configuration): Observable[Row] = {

    val reader = ParquetReaderFn.apply(path, predicate, Option(projectionSchema))
    Observable.apply { it =>
      try {
        it.onStart()
        ParquetRowIterator(reader).takeWhile(_ => !it.isUnsubscribed).foreach(it.onNext)
        it.onCompleted()
      } catch {
        case NonFatal(e) =>
          it.onError(e)
      }
    }
  }

  override def writer(schema: Schema,
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