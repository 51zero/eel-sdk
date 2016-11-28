package io.eels.component.hive.dialect

import com.sksamuel.exts.Logging
import io.eels.Row
import io.eels.component.avro.{AvroSchemaFns, RecordSerializer}
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.component.parquet._
import io.eels.schema.StructType
import io.reactivex.Flowable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.reactivestreams.{Publisher, Subscriber, Subscription}

import scala.util.control.NonFatal

object ParquetHiveDialect extends HiveDialect with Logging {

  override def read(path: Path,
                    metastoreSchema: StructType,
                    projectionSchema: StructType,
                    predicate: Option[Predicate])
                   (implicit fs: FileSystem, conf: Configuration): Flowable[Row] = {

    val schema = AvroSchemaFns.toAvroSchema(projectionSchema)

    Flowable.fromPublisher(new Publisher[Row] {
      override def subscribe(s: Subscriber[_ >: Row]): Unit = {
        s.onSubscribe(new Subscription {

          val reader = ParquetReaderFn.apply(path, predicate, Option(schema))
          val iter = ParquetRowIterator(reader)

          override def cancel(): Unit = reader.close()
          override def request(n: Long): Unit = {
            try {
              for (_ <- 1L to n)
                if (iter.hasNext)
                  s.onNext(iter.next)
                else
                  s.onComplete()
            } catch {
              case NonFatal(e) =>
                s.onError(e)
                reader.close()
            }
          }
        })
      }
    })
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