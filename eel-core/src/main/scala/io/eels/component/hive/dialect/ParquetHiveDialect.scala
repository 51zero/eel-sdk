package io.eels.component.hive.dialect

import com.sksamuel.exts.Logging
import io.eels.{Predicate, Row}
import io.eels.component.avro.{AvroRecordSerializer, AvroSchemaFns}
import io.eels.component.hive.HiveDialect
import io.eels.component.hive.HiveWriter
import io.eels.component.parquet.{ParquetLogMute, ParquetReaderFn, ParquetRowIterator, ParquetRowWriter}
import io.eels.schema.Schema
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import rx.lang.scala.Observable

object ParquetHiveDialect extends HiveDialect with Logging {

  override def read(path: Path,
                    metastoreSchema: Schema,
                    projectionSchema: Schema,
                    predicate: Option[Predicate])(implicit fs: FileSystem): Observable[Row] = {

    val reader = ParquetReaderFn.apply(path, predicate, Option(projectionSchema))
    Observable.apply { subscriber =>
      subscriber.onStart()
      ParquetRowIterator(reader).foreach(subscriber.onNext)
      subscriber.onCompleted()
    }
  }

  override def writer(schema: Schema,
                      path: Path)
                     (implicit fs: FileSystem): HiveWriter = new HiveWriter {
      ParquetLogMute()

    // hive is case insensitive so we must lower case the fields to keep it consistent
    val avroSchema = AvroSchemaFns.toAvroSchema(schema, caseSensitive = false)
    val writer = new ParquetRowWriter(path, avroSchema)
    val serializer = new AvroRecordSerializer(avroSchema)

    override def write(row: Row) {
      val record = serializer.toRecord(row)
      writer.write(record)
    }

    override def close() {
      logger.debug("Closing dialect writer")
      writer.close()
    }
  }
}