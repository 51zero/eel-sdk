package io.eels.datastream

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, LinkedBlockingQueue}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
import com.sksamuel.exts.io.Using
import io.eels.schema.StructType
import io.eels.{Row, Source}

object ExecutorInstances {
  val io = Executors.newCachedThreadPool()
}

// subscribes to a part and publishes to a shared queue
// once this part finishes, if it is the last part completed then it
// will trigger completion to the queue via a sentinel
class PartSubscriber(name: String,
                     queue: LinkedBlockingQueue[Seq[Row]],
                     outstanding: AtomicInteger) extends Subscriber[Seq[Row]] with Logging {

  // to cancel the part publisher
  var cancellable: Cancellable = null

  override def starting(c: Cancellable): Unit = {
    logger.debug(s"Starting reads for part $name")
    cancellable = c
  }

  override def completed(): Unit = {
    logger.debug(s"Part $name has finished")
    if (outstanding.decrementAndGet() == 0) {
      logger.debug("All parts have finished; marking queue with sentinel")
      queue.put(Row.Sentinel)
    }
  }

  // in the case of an error in a downstream part, we'll terminate the
  // downsteam subscriber immediately
  override def error(t: Throwable): Unit = {
    logger.error(s"Error reading part $name; terminating queue", t)
    queue.put(Row.Sentinel)
  }

  override def next(t: Seq[Row]): Unit = queue.put(t)
}

// an implementation of DataStream that provides a subscribe powered by constitent parts
class DataStreamSource(source: Source) extends DataStream with Using with Logging {

  private val bufferSize = 100

  override def schema: StructType = source.schema

  override def subscribe(s: Subscriber[Seq[Row]]): Unit = {

    val parts = source.parts()
    if (parts.isEmpty) {

      logger.info("No parts for this source; immediate completion")
      s.starting(Cancellable.empty)
      s.completed()

    } else {

      val queue = new LinkedBlockingQueue[Seq[Row]](bufferSize)
      val outstanding = new AtomicInteger(parts.size)

      // create a subscriber for each part
      // each part should be read in its own io thread
      val subscribers = parts.zipWithIndex.map { case (part, k) =>
        val subscriber = new PartSubscriber(k.toString, queue, outstanding)
        ExecutorInstances.io.execute(new Runnable {
          override def run(): Unit = {
            try {
              part.subscribe(subscriber)
            } catch {
              case t: Throwable =>
                logger.error(s"Error subscribing to part $k", t)
                queue.put(Row.Sentinel)
            }
          }
        })
        subscriber
      }

      // this cancellable can be used by the downstream subscriber
      // to cancel all the constituent parts
      val cancellable = new Cancellable {
        override def cancel(): Unit = {
          subscribers.map(_.cancellable).filter(_ != null).foreach(_.cancel)
        }
      }

      try {
        s.starting(cancellable)
        BlockingQueueConcurrentIterator(queue, Row.Sentinel).foreach(s.next)
        s.completed()
      } catch {
        case t: Throwable =>
          logger.error("Error processing queue", t)
          cancellable.cancel()
          s.error(t)
      }
    }
  }
}

trait Cancellable {
  def cancel()
}

object Cancellable {
  val empty = new Cancellable {
    override def cancel(): Unit = ()
  }
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