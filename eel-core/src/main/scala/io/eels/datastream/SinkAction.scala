package io.eels.datastream

import java.util.concurrent.atomic.{AtomicLong, AtomicReference, LongAdder}
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
import io.eels.{Row, Sink}

case class SinkAction(ds: DataStream, sink: Sink, parallelism: Int) extends Logging {

  def execute(): Long = {

    val schema = ds.schema
    val adder = new LongAdder

    val failure = new AtomicReference[Throwable](null)

    val executor = Executors.newFixedThreadPool(parallelism)
    val queue = new LinkedBlockingQueue[Seq[Row]](DataStream.DefaultBufferSize)
    val completed = new AtomicLong(0)
    var subscription: Subscription = null

    val subscriber = new Subscriber[Seq[Row]] {
      override def subscribed(sub: Subscription): Unit = {
        logger.debug(s"Subscribing to datastream for sink action [subscription=$sub]")
        subscription = sub
      }

      override def next(chunk: Seq[Row]): Unit = {
        if (failure.get == null)
          queue.put(chunk)
      }

      override def completed(): Unit = {
        logger.debug("Sink action has received all chunks")
        queue.put(Row.Sentinel)
      }

      override def error(t: Throwable): Unit = {
        logger.error("Sink subscriber has received error", t)
        // if we have an error from downstream
        failure.set(t)
        // clear the queue on error so always room for the sentinel
        queue.clear()
        queue.put(Row.Sentinel)
        executor.shutdownNow()
        subscription.cancel()
      }
    }

    val writers = sink.open(schema, parallelism)
    logger.debug(s"Opened up ${writers.size} writers from sink $sink")

    val tasks = writers.zipWithIndex.map { case (writer, k) =>
      new Runnable {
        override def run(): Unit = {
          logger.info(s"Starting thread writer $k")
          try {
            BlockingQueueConcurrentIterator(queue, Row.Sentinel).foreach { chunk =>
              chunk.foreach { row =>
                writer.write(row)
                adder.increment()
              }
            }
          } catch {
            case t: Throwable =>
              logger.error(s"Error writing chunk ${completed.incrementAndGet}: ${t.getMessage}; cancelling upstream")
              queue.clear()
              queue.put(Row.Sentinel)
              failure.compareAndSet(null, t)
              subscription.cancel()
          }
          logger.info(s"Ending thread writer $k")
        }
      }
    }

    tasks.foreach(executor.execute)
    executor.shutdown()

    ds.subscribe(subscriber)

    // at this point, the subscriber has returned, and now we need to wait until
    // all outstanding write tasks complete
    logger.info("Waiting for executor to terminate")
    executor.awaitTermination(999, TimeUnit.DAYS)
    logger.info(s"Sink has written ${adder.sum} rows")

    // we close all writers together so its atomic-ish
    logger.info("Closing all sink writers...")
    writers.foreach(_.close)
    logger.info("All sink writers are closed")

    if (failure.get != null)
      throw failure.get

    val sum = adder.sum()
    logger.info(s"Sink wrote $sum records")
    sum
  }
}