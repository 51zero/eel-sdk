package io.eels.datastream

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
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

    val queue = new LinkedBlockingQueue[Seq[Row]](100)

    val finished = new AtomicLong(0)
    val parts = source.parts()

    val running = new AtomicBoolean(true)
    val cancellable = new Cancellable {
      override def cancel(): Unit = running.set(false)
    }

    s.starting(cancellable)

    if (parts.isEmpty) {
      logger.info("No parts for this source; immediate completion")
      s.completed()
    } else {
      class PartSubscriber(name: String) extends Subscriber[Seq[Row]] {

        var cancellable: Cancellable = null

        override def starting(c: Cancellable): Unit = {
          logger.debug(s"Starting reads for part $name")
          cancellable = c
        }

        override def completed(): Unit = {
          logger.debug(s"Part $name has finished")
          if (finished.incrementAndGet == parts.size)
            queue.put(Row.Sentinel)
        }

        override def error(t: Throwable): Unit = {
          logger.error(s"Error reading part $name", t)
          if (cancellable != null)
            cancellable.cancel()
          else {
            logger.warn("Cancellable was null")
          }
          if (finished.incrementAndGet == parts.size)
            queue.put(Row.Sentinel)
        }

        override def next(t: Seq[Row]): Unit = {
          queue.put(t)
          // if we've been told to stop running, then we'll cancel downstream
          if (!running.get) {
            logger.debug(s"Cancelling part $name", t)
            if (cancellable != null)
              cancellable.cancel()
          }
        }
      }

      // each part should be read in its own io thread
      parts.zipWithIndex.foreach { case (part, k) =>
        ExecutorInstances.io.execute(new Runnable {
          override def run(): Unit = {
            try {
              part.subscribe(new PartSubscriber(k.toString))
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
}

trait Cancellable {
  def cancel()
}

trait Subscriber[T] {
  // notifies the subscriber that the publisher is about to begin
  // the given cancellable can be used to stop the publisher
  def starting(c: Cancellable)
  def error(t: Throwable)
  def next(t: T)
  def completed()
}

class DelegateSubscriber[T](delegate: Subscriber[T]) extends Subscriber[T] {
  override def starting(c: Cancellable): Unit = delegate.starting(c)
  override def completed(): Unit = delegate.completed()
  override def error(t: Throwable): Unit = delegate.error(t)
  override def next(t: T): Unit = delegate.next(t)
}