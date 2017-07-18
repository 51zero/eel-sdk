package io.eels.datastream

import java.util.concurrent.LinkedBlockingQueue

import io.eels.Row
import io.eels.schema.StructType

/**
  * An implementation of DataStream for which items are emitted by calling publish.
  * When no more items are to be published, call close() so that downstream subscribers can complete.
  *
  * Subscribers to this publisher will block as normal, and so they should normally be placed
  * into a separate thread.
  */
class DataStreamPublisher(override val schema: StructType) extends DataStream {

  private val queue = new LinkedBlockingQueue[Row]

  override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
    try {
      subscriber.starting(new Cancellable {
        override def cancel(): Unit = queue.put(Row.SentinelSingle)
      })
      Iterator.continually(queue.take).takeWhile(_ != Row.SentinelSingle).grouped(1000).foreach(subscriber.next)
      subscriber.completed()
    } catch {
      case t: Throwable => subscriber.error(t)
    }
  }

  def publish(row: Row): Unit = queue.put(row)

  def close(): Unit = queue.add(Row.SentinelSingle)
}
