package io.eels.component.hive.dialect

import com.sksamuel.exts.Logging
import io.eels.Row
import io.eels.component.Predicate
import io.eels.component.avro.AvroRecordSerializer
import io.eels.component.avro.AvroSchemaFns
import io.eels.component.hive.HiveDialect
import io.eels.component.hive.HiveWriter
import io.eels.component.parquet.ParquetRowIterator
import io.eels.component.parquet.ParquetLogMute
import io.eels.component.parquet.ParquetReaderFns
import io.eels.component.parquet.ParquetRowWriter
import io.eels.schema.Schema
import io.eels.util.Logging
import io.eels.util.Option
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import rx.Observable

object ParquetHiveDialect extends HiveDialect with Logging {

  override def read(path: Path,
                    metastoreSchema: Schema,
                    projectionSchema: Schema,
                    predicate: Option[Predicate],
                    fs: FileSystem): Observable[Row] = {

    val reader = ParquetReaderFns.createReader(path, predicate, Option(projectionSchema))
    Observable.create < Row > {
      subscriber ->
      subscriber.onStart()
      ParquetRowIterator(reader).forEach {
        subscriber.onNext(it)
      }
      subscriber.onCompleted()
    }
  }

  override def writer(schema: Schema,
                      path: Path,
                      fs: FileSystem): HiveWriter = new HiveWriter {
      ParquetLogMute()

    // hive is case insensitive so we must lower case the fields everything to keep it consistent
    val avroSchema = AvroSchemaFns.toAvroSchema(schema, caseSensitive = false)
    val writer = ParquetRowWriter(path, avroSchema, fs)
    val serializer = AvroRecordSerializer(avroSchema)

    override fun write(row: Row) {
      val record = serializer.toRecord(row)
      writer.write(record)
    }

    override fun close() {
      logger.debug("Closing dialect writer")
      writer.close()
    }
  }
}