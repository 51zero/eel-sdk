package io.eels.component.parquet

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import com.sksamuel.exts.io.Using
import io.eels.component.parquet.util.ParquetIterator
import io.eels.datastream.{Cancellable, Publisher, Subscriber}
import io.eels.schema.StructType
import io.eels.{Predicate, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.MessageType

class ParquetPublisher(path: Path,
                       predicate: Option[Predicate],
                       projection: Seq[String],
                       caseSensitive: Boolean,
                       dictionaryFiltering: Boolean)
                      (implicit conf: Configuration) extends Publisher[Seq[Row]] with Logging with Using {

  def readSchema: Option[MessageType] = {
    if (projection.isEmpty) None
    else {

      val fileSchema = ParquetFileReader.open(conf, path).getFileMetaData.getSchema
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

  override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
    using(RowParquetReaderFn(path, predicate, readSchema, dictionaryFiltering)) { reader =>
      try {
        var cancelled = false
        subscriber.starting(new Cancellable {
          override def cancel(): Unit = cancelled = true
        })
        ParquetIterator(reader).takeWhile(_ => !cancelled).grouped(1000).foreach(subscriber.next)
        subscriber.completed()
      } catch {
        case t: Throwable => subscriber.error(t)
      }
    }
  }
}