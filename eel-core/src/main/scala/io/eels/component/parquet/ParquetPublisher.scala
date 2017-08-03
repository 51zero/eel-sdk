package io.eels.component.parquet

import java.util.concurrent.atomic.AtomicBoolean

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import com.sksamuel.exts.io.Using
import io.eels.{Chunk, Predicate}
import io.eels.component.parquet.util.ParquetIterator
import io.eels.datastream.{DataStream, Publisher, Subscriber, Subscription}
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.MessageType

class ParquetPublisher(path: Path,
                       predicate: Option[Predicate],
                       projection: Seq[String],
                       caseSensitive: Boolean,
                       dictionaryFiltering: Boolean)
                      (implicit conf: Configuration) extends Publisher[Chunk] with Logging with Using {

  def readSchema: Option[MessageType] = {
    if (projection.isEmpty) None
    else {

      val fileSchema = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER).getFileMetaData.getSchema
      val structType = ParquetSchemaFns.fromParquetMessageType(fileSchema)

      if (caseSensitive) {
        assert(
          structType.fieldNames.toSet.size == structType.fieldNames.map(_.toLowerCase).toSet.size,
          "Cannot use case sensitive = true when this would result in a clash of field names"
        )
      }

      val projectionSchema = StructType(projection.map { field =>
        structType.field(field, caseSensitive).getOrError(s"Requested field $field does not exist in the parquet schema")
      })

      ParquetSchemaFns.toParquetMessageType(projectionSchema).some
    }
  }

  override def subscribe(subscriber: Subscriber[Chunk]): Unit = {
    try {
      using(RecordParquetReaderFn(path, predicate, readSchema, dictionaryFiltering)) { reader =>
        val running = new AtomicBoolean(true)
        subscriber.subscribed(Subscription.fromRunning(running))
        ParquetIterator(reader)
          .takeWhile(_ => running.get)
          .grouped(DataStream.DefaultBatchSize)
          .foreach(subscriber.next)
        subscriber.completed()
      }
    } catch {
      case t: Throwable => subscriber.error(t)
    }
  }
}