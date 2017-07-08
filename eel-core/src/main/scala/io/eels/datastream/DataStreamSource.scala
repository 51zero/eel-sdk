package io.eels.datastream

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, LinkedBlockingQueue}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
import com.sksamuel.exts.io.Using
import io.eels.schema.StructType
import io.eels.{Row, Source}

object ExecutorInstances {
  val io = Executors.newCachedThreadPool()
}

// an implementation of DataStream that provides a subscribe powered by constitent parts
class DataStreamSource(source: Source) extends DataStream with Using with Logging {

  override def schema: StructType = source.schema

  override def subscribe(s: Subscriber[Seq[Row]]): Unit = {

    val queue = new LinkedBlockingQueue[Seq[Row]](1000)

    val finished = new AtomicLong(0)
    val parts = source.parts()

    // each part should be read in its own io thread
    parts.zipWithIndex.foreach { case (part, k) =>
      ExecutorInstances.io.execute(new Runnable {
        override def run(): Unit = {
          try {
            part.subscribe(new Subscriber[Seq[Row]] {
              logger.debug(s"Starting reads for part $k")
              override def next(t: Seq[Row]): Unit = queue.put(t)
              override def started(c: Cancellable): Unit = ()
              override def completed(): Unit = {
                logger.debug(s"Part $k has finished")
                if (finished.incrementAndGet == parts.size)
                  queue.put(Row.Sentinel)
              }
              override def error(t: Throwable): Unit = {
                logger.error(s"Error reading part $k", t)
                if (finished.incrementAndGet == parts.size)
                  queue.put(Row.Sentinel)
              }
            })
          } catch {
            case t: Throwable =>
              logger.error(s"Error subscribing to part $k", t)
              queue.put(Row.Sentinel)
          }
        }
      })
    }

    try {
      BlockingQueueConcurrentIterator(queue, Row.Sentinel).foreach(s.next)
      s.completed()
    } catch {
      case t: Throwable => s.error(t)
    }
  }
}

trait Cancellable {
  def cancel()
}

trait Subscriber[T] {
  def started(c: Cancellable)
  def completed()
  def error(t: Throwable)
  def next(t: T)
}

class DelegateSubscriber[T](delegate: Subscriber[T]) extends Subscriber[T] {
  override def started(c: Cancellable): Unit = delegate.started(c)
  override def completed(): Unit = delegate.completed()
  override def error(t: Throwable): Unit = delegate.error(t)
  override def next(t: T): Unit = delegate.next(t)
}