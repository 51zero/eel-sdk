package io.eels

import com.typesafe.config.ConfigFactory
import io.eels.component.Using
import io.eels.schema.Schema
import io.eels.util.Logging
import rx.Observable
import rx.Subscriber
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class FrameSource(val ioThreads: Int, val source: Source) : Frame, Logging, Using {

  private val config = ConfigFactory.load()
  private val bufferSize by lazy {
    val bufferSize = config.getInt("eel.source.defaultBufferSize")
    logger.info("FrameSource is configured with bufferSize=$bufferSize")
    bufferSize
  }

  override fun schema(): Schema = source.schema()

  // this method may be invoked multiple times, each time generating a new "load action" and a new
  // resultant observable from it.
  // todo is this is the behaviour I want?
  override fun observable(): Observable<Row> {

    // the number of ioThreads here will determine how we parallelize the loading of data
    // into the observable. Each thread will read from a single part, so the more threads
    // the more parts will be read concurrently.
    //
    // Each thread will read its part into an intermediate queue, which we use in order to eagerly
    // buffer values, and the queue is read from by the observable to produce values.
    //
    // Todo we could perhaps just combine the part observables using the built in combinators and have each part do eager reads
    val executor = Executors.newFixedThreadPool(ioThreads)
    logger.info("Source will read using a FixedThreadPool [ioThreads=$ioThreads]")

    val parts = source.parts()
    logger.info("Source has ${parts.size} parts")

    // faster to use a linked blocking queue than an array one
    // because a linkedbq has a lock for both head and tail so allows us to push and pop at same time
    // very useful for our pipeline
    val queue = LinkedBlockingQueue<Row>(bufferSize)
    val count = AtomicLong(0)
    val latch = CountDownLatch(parts.size)

    for (part in parts) {
      executor.submit {
        part.data().subscribe(object : Subscriber<Row>() {

          override fun onNext(row: Row?) {
            queue.put(row)
          }

          override fun onError(e: Throwable?) {
            logger.error("Error while loading from source part; the remaining data in this part will be skipped", e)
            latch.countDown()
          }

          override fun onCompleted() {
            logger.info("Completed part #${count.incrementAndGet()}")
            latch.countDown()
          }
        })
      }
    }

    executor.submit {
      latch.await(1, TimeUnit.DAYS)
      logger.info("All source parts completed; latch released")
      try {
        queue.put(Row.PoisonPill)
        logger.debug("PoisonPill added to source queue to close source observable")
      } catch (e: Exception) {
        logger.error("Error adding PoisonPill", e)
      }
    }
    executor.shutdown()

    return Observable.create { subscriber ->
      subscriber.onStart()
      queue.takeWhile { it != Row.PoisonPill }.forEach {
        subscriber.onNext(it)
      }
      subscriber.onCompleted()
    }
  }
}