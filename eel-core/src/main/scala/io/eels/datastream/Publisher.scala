package io.eels.datastream

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{ExecutorService, LinkedBlockingQueue}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
import com.sksamuel.exts.concurrent.ExecutorImplicits._

import scala.collection.mutable.ArrayBuffer

trait Publisher[T] {
  def subscribe(subscriber: Subscriber[T])
}

object Publisher extends Logging {

  /**
    * Returns a new Publisher which will stream elements from the given upstream publishers as
    * a single merged publisher.
    */
  def merge[T](publishers: Seq[Publisher[T]], sentinel: T)(implicit executor: ExecutorService): Publisher[T] = {

    new Publisher[T] {
      override def subscribe(s: Subscriber[T]): Unit = {

        // subscribers to the returned publisher will be fed from an intermediate queue
        val queue = new LinkedBlockingQueue[Either[Throwable, T]](DataStream.DefaultBufferSize)

        // to keep track of how many subscribers are yet to finish; only once all upstream
        // publishers have finished will this subscriber be completed.
        val outstanding = new AtomicInteger(publishers.size)

        // we make a collection of all the subscriptions, so if there's an error at any point in the
        // merge, we can cancel all upstream producers
        val subscriptions = ArrayBuffer.empty[Subscription]

        // this cancellable can be used to cancel all the subscriptions
        val subscription = new Subscription {
          override def cancel(): Unit = subscriptions.foreach(_.cancel)
        }

        // status flag that an error occured and the subscriptions should watch for it
        val error = new AtomicReference[Throwable](null)
        def terminate(t: Throwable): Unit = {
          logger.error(s"Error in merge", t)
          error.set(t)
          subscription.cancel()
          queue.clear()
          queue.put(Right(sentinel))
        }

        // each subscriber will occupy its own thread, on the provided executor
        publishers.foreach { publisher =>
          executor.submit {
            try {
              publisher.subscribe(new Subscriber[T] {
                override def subscribed(sub: Subscription): Unit = if (sub != null) subscriptions.append(sub)
                override def next(t: T): Unit = queue.put(Right(t))
                override def error(t: Throwable): Unit = terminate(t)
                override def completed(): Unit = {
                  if (outstanding.decrementAndGet() == 0) {
                    logger.debug("All subscribers have finished; marking queue with sentinel")
                    queue.put(Right(sentinel))
                  }
                }
              })
            } catch {
              case t: Throwable => terminate(t)
            }
          }
        }

        try {
          s.subscribed(subscription)
          BlockingQueueConcurrentIterator(queue, Right(sentinel)).takeWhile(_ => error.get == null).foreach {
            case Left(t) => s.error(t)
            case Right(t) => s.next(t)
          }
          // once we've had an error that's it, we don't complete the subscriber
          if (error.get == null)
            s.completed()
        } catch {
          case t: Throwable =>
            logger.error("Error in merge subscriber", t)
            subscription.cancel()
            s.error(t)
        }

        logger.debug("Merge subscriber has completed")
      }
    }
  }
}