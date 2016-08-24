package io.eels

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, Executors, LinkedBlockingQueue, TimeUnit}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import com.typesafe.config.ConfigFactory
import io.eels.schema.Schema
import rx.lang.scala.{Observable, Observer, Subscriber}

class FrameSource(ioThreads: Int,
                  source: Source,
                  observer: Observer[Row] = NoopObserver) extends Frame with Logging with Using {

  private val config = ConfigFactory.load()
  private val bufferSize = config.getInt("eel.source.defaultBufferSize")
  logger.info(s"FrameSource is configured with bufferSize=$bufferSize")

  override def schema(): Schema = source.schema()

  // this method may be invoked multiple times, each time generating a new "load action" and a new
  // resultant rows from it.
  // todo is this is the behaviour I want?
  override def rows(): Observable[Row] = {

    // the number of ioThreads here will determine how we parallelize the loading of data
    // into the rows. Each thread will read from a single part, so the more threads
    // the more parts will be read concurrently.
    //
    // Each thread will read its part into an intermediate queue, which we use in order to eagerly
    // buffer values, and the queue is read from by the rows to produce values.
    //
    val executor = Executors.newFixedThreadPool(ioThreads)
    logger.info(s"Source will read using a FixedThreadPool [ioThreads=$ioThreads]")

    val parts = source.parts()
    logger.info(s"Source has ${parts.size} parts")

    // faster to use a LinkedBlockingQueue than an array one
    // because a LinkedBlockingQueue has a lock for both head and tail so
    // allows us to push and pop at same time
    // very useful for our pipeline
    val queue = new LinkedBlockingQueue[Row](bufferSize)
    val count = new AtomicLong(0)
    val latch = new CountDownLatch(parts.size)

    for (part <- parts) {
      import com.sksamuel.exts.concurrent.ExecutorImplicits._
      executor.submit {

        val id = count.incrementAndGet()

        part.data().subscribe(new Subscriber[Row]() {
          override def onStart() {
            logger.info(s"Starting part #$id")
          }

          override def onNext(row: Row) {
            queue.put(row)
            observer.onNext(row)
          }

          override def onError(e: Throwable) {
            logger.error(s"Error while loading from part #$id; the remaining data in this part will be skipped", e)
            latch.countDown()
            observer.onError(e)
          }

          override def onCompleted() {
            logger.info(s"Completed part #$id")
            latch.countDown()
            observer.onCompleted()
          }
        })
      }
    }

    import com.sksamuel.exts.concurrent.ExecutorImplicits._
    executor.submit {
      latch.await(1, TimeUnit.DAYS)
      logger.info("All source parts completed; latch released")
      try {
        queue.put(Row.Sentinel)
        logger.debug("PoisonPill added to source queue to close source rows")
      } catch {
        case t: Throwable =>
          logger.error("Error adding PoisonPill", t)
      }
    }
    executor.shutdown()

    Observable { subscriber =>
      subscriber.onStart()
      var running = true
      while (running) {
        val next = queue.take()
        next match {
          case Row.Sentinel =>
            logger.debug("Poison pill detected by rows, notifying subscriber of end of data")
            subscriber.onCompleted()
            running = false
          case _ =>
            subscriber.onNext(next)
        }
      }
    }
  }
}