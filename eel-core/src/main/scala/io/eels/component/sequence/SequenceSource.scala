package io.eels.component.sequence

import java.util.concurrent.atomic.AtomicBoolean

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels._
import io.eels.datastream.{DataStream, Publisher, Subscriber, Subscription}
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable, SequenceFile}

case class SequenceSource(path: Path)(implicit conf: Configuration) extends Source with Logging {
  logger.debug(s"Creating sequence source from $path")

  override def schema: StructType = SequenceSupport.schema(path)
  override def parts(): Seq[Publisher[Seq[Row]]] = List(new SequencePublisher(path))
}

object SequenceReaderIterator {
  def apply(schema: StructType, reader: SequenceFile.Reader): Iterator[Row] = new Iterator[Row] {
    private val k = new IntWritable()
    private val v = new BytesWritable()
    // throw away the header
    reader.next(k, v)
    override def next(): Row = Row(schema, SequenceSupport.toValues(v).toVector)
    override def hasNext(): Boolean = reader.next(k, v)
  }
}

class SequencePublisher(val path: Path)(implicit conf: Configuration) extends Publisher[Seq[Row]] with Logging with Using {

  override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
    try {
      using(SequenceSupport.createReader(path)) { reader =>
        val schema = SequenceSupport.schema(path)
        val running = new AtomicBoolean(true)
        subscriber.subscribed(Subscription.fromRunning(running))
        SequenceReaderIterator(schema, reader)
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