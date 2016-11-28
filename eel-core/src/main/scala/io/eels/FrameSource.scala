package io.eels

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import com.sksamuel.exts.Logging
import com.sksamuel.exts.concurrent.ExecutorImplicits._
import com.typesafe.config.ConfigFactory
import io.eels.schema.StructType
import io.reactivex.Flowable
import io.reactivex.functions.Consumer
import io.reactivex.schedulers.Schedulers

import scala.util.control.NonFatal

class FrameSource(source: Source) extends Logging {

  def load(executor: ExecutorService, _listener: Listener): Frame = new Frame {

    override def schema(): StructType = source.schema()

    // this method may be invoked multiple times, each time generating a new "load action"
    override def rows(): Flowable[Row] = {
      val reader = new ConcurrentPartsReader(executor, _listener, source.parts())
      val publisher = new BlockingQueuePublisher[Row](reader.queue, Row.Sentinel)
      reader.start()
      Flowable.fromPublisher(publisher)
    }
  }
}

// the executor is used to parallelize the loading of data.
// Each thread will read from a single part, so the more threads
// the more parts will be read concurrently.
//
// Each thread will pump its values into a blocking queue which is used
// by the subscription to feed the resultant Flowable.
class ConcurrentPartsReader(executor: ExecutorService,
                            listener: Listener,
                            parts: Seq[Part]) extends Logging {
  logger.info(s"Source has ${parts.size} part(s)")

  private val config = ConfigFactory.load()
  private val bufferSize = config.getInt("eel.source.defaultBufferSize")
  logger.info(s"FrameSource is configured with bufferSize=$bufferSize")

  // it is faster to use a LinkedBlockingQueue than an array one
  // because a LinkedBlockingQueue has a lock for both head and tail so
  // allows us to push and pop at same time
  // very useful for our pipeline
  val queue = new LinkedBlockingQueue[Row](bufferSize)
  val latch = new CountDownLatch(parts.size)
  def start(): Unit = {

    val ids = new AtomicInteger(0)

    for (part <- parts) {

      // we create a task per part, and each task just reads from that part putting the data
      // into the shared buffer. The more threads we allocate to this the more tasks (parts)
      // we can process concurrently.
      executor.submit {
        val id = ids.incrementAndGet()
        try {
          part.data().observeOn(Schedulers.newThread())
          part.data().blockingSubscribe(new Consumer[Row] {
            override def accept(t: Row): Unit = {
              queue.put(t)
            }
          })
        } catch {
          case NonFatal(e) =>
            logger.error(s"Error while reading part #$id; the remaining rows will be skipped", e)
        } finally {
          latch.countDown()
        }
      }
    }

    executor.submit {
      executor.shutdown()
      latch.await(100, TimeUnit.DAYS)
      logger.info("All source parts completed")

      try {
        // we add a sentinel so that readers of our queue know when the queue has finished.
        // since it is blocking, without a sentinel or an interupt it would block forever
        logger.debug("Sending sentinel to source row queue")
        queue.put(Row.Sentinel)
      } catch {
        case NonFatal(e) =>
          logger.error("Error adding Sentinel", e)
          throw e
      }
    }
  }
}