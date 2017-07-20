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
    val queue = new LinkedBlockingQueue[Seq[Row]](100)
    val completed = new AtomicLong(0)
    var dscancellable: Cancellable = null

    val subscriber = new Subscriber[Seq[Row]] {
      override def starting(c: Cancellable): Unit = {
        logger.debug(s"Subscribing to datastream for sink action [cancellable=$c]")
        dscancellable = c
      }

      override def next(chunk: Seq[Row]): Unit = {
        queue.put(chunk)
      }

      override def completed(): Unit = {
        logger.debug("Sink action has received all chunks")
        queue.put(Row.Sentinel)
      }

      override def error(t: Throwable): Unit = {
        // if we have an error from downstream
        executor.shutdownNow()
        failure.set(t)
      }
    }

    val writers = sink.open(schema, parallelism)
    writers.zipWithIndex.foreach { case (writer, k) =>
      executor.submit(new Runnable {
        override def run(): Unit = {
          logger.info(s"Starting thread writer $k")
          try {
            BlockingQueueConcurrentIterator(queue, Row.Sentinel).takeWhile(_ => failure.get == null).foreach { chunk =>
              chunk.foreach { row =>
                writer.write(row)
                adder.increment()
              }
              logger.debug(s"Chunk ${completed.incrementAndGet} has completed")
            }
          } catch {
            case t: Throwable =>
              logger.error(s"Error writing chunk ${completed.incrementAndGet}", t)
              failure.set(t)
              // if we have an error writing, we'll exit immediately by cancelling the downstream
              dscancellable.cancel()
          }
          logger.info(s"Ending thread writer $k")
        }
      })
    }
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

    adder.sum()
  }
}