package io.eels.datastream

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicMarkableReference, AtomicReference}

import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
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

  private val queue = new LinkedBlockingQueue[Seq[Row]]
  private val running = new AtomicBoolean(true)
  private val failure = new AtomicReference[Throwable](null)

  def isCancelled: Boolean = !running.get

  override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
    try {
      subscriber.subscribed(new Subscription {
        override def cancel(): Unit = {
          queue.clear()
          queue.put(Row.Sentinel)
          running.set(false)
        }
      })
      BlockingQueueConcurrentIterator(queue, Row.Sentinel).takeWhile(_ => running.get).foreach(subscriber.next)
      failure.get match {
        case t: Throwable => subscriber.error(t)
        case _ => subscriber.completed()
      }
    } catch {
      case t: Throwable => subscriber.error(t)
    }
  }

  def publish(row: Seq[Row]): Unit = queue.put(row)
  def error(t: Throwable): Unit = {
    failure.set(t)
    queue.clear()
    queue.add(Row.Sentinel)
  }
  def close(): Unit = queue.add(Row.Sentinel)
}
