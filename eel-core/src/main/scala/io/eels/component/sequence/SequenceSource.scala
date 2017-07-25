package io.eels.component.sequence

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels._
import io.eels.datastream.{DataStream, Publisher, Subscriber}
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable}

case class SequenceSource(path: Path)(implicit conf: Configuration) extends Source with Logging {
  logger.debug(s"Creating sequence source from $path")

  override def schema: StructType = SequenceSupport.schema(path)
  override def parts(): Seq[Publisher[Seq[Row]]] = List(new SequencePublisher(path))
}

class SequencePublisher(val path: Path)(implicit conf: Configuration) extends Publisher[Seq[Row]] with Logging with Using {

  override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
    using(SequenceSupport.createReader(path)) { reader =>
      try {

        val reader = SequenceSupport.createReader(path)
        val k = new IntWritable()
        val v = new BytesWritable()
        val schema = SequenceSupport.schema(path)

        // throw away the header
        //     reader.next(k, v)

        val iterator: Iterator[Row] = new Iterator[Row] {
          override def next(): Row = Row(schema, SequenceSupport.toValues(v).toVector)
          override def hasNext(): Boolean = reader.next(k, v)
        }

        iterator.grouped(DataStream.batchSize).foreach(subscriber.next)
        subscriber.completed()
      } catch {
        case t: Throwable => subscriber.error(t)
      }
    }
  }
}