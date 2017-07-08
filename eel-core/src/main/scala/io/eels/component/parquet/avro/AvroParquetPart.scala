package io.eels.component.parquet.avro

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels.component.avro.AvroDeserializer
import io.eels.component.parquet.util.ParquetIterator
import io.eels.datastream.Subscriber
import io.eels.{Part, Predicate, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

class AvroParquetPart(path: Path,
                      predicate: Option[Predicate])(implicit conf: Configuration)
  extends Part with Logging with Using {

  override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
    using(AvroParquetReaderFn(path, predicate, None)) { reader =>
      try {
        val deser = new AvroDeserializer()
        val iterator = ParquetIterator(reader).map(deser.toRow)

        iterator.grouped(1000).foreach(subscriber.next)
        subscriber.completed()
      } catch {
        case t: Throwable => subscriber.error(t)
      }
    }
  }
}