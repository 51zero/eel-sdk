package io.eels.datastream

import java.util.concurrent.atomic.{AtomicLong, LongAdder}
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}

import com.sksamuel.exts.Logging
import io.eels.{Row, Sink}

case class SinkAction(ds: DataStream, sink: Sink, parallelism: Int) extends Logging {

  def execute(): Long = {

    val schema = ds.schema
    val adder = new LongAdder

    var failure: Throwable = null

    val executor = Executors.newFixedThreadPool(parallelism)
    val queue = new LinkedBlockingQueue[Seq[Row]](100)
    val completed = new AtomicLong(0)

    val writers = sink.open(schema, parallelism)
    writers.zipWithIndex.foreach { case (writer, k) =>
      executor.submit(new Runnable {
        override def run(): Unit = {
          logger.info(s"Starting thread writer $k")
          while (true) {
            val chunk = queue.take()
            try {
              chunk.foreach { row =>
                writer.write(row)
                adder.increment()
              }
            } catch {
              case t: Throwable =>
                logger.error("Error writing to stream", t)
                // if we have an error writing, we'll exit all writers immediately
                executor.shutdownNow()
                failure = t
            } finally {
              logger.debug(s"Chunk ${completed.incrementAndGet} has completed")
            }
          }
        }
      })
    }
    executor.shutdown()

    ds.subscribe(new Subscriber[Seq[Row]] {

      var cancellable: Cancellable = null

      override def starting(c: Cancellable): Unit = {
        logger.debug(s"Subscribing to datastream for sink action [cancellable=$c]")
        this.cancellable = c
      }

      override def next(chunk: Seq[Row]): Unit = queue.put(chunk)

      override def completed(): Unit = {
        logger.debug("Sink action has received all chunks")
        queue.put(Row.Sentinel)
      }

      override def error(t: Throwable): Unit = {
        // if we have an error from downstream, we'll exit immediately and cancel downstream
        if (cancellable != null)
          cancellable.cancel()
        executor.shutdownNow()
        failure = t
      }
    })

    // at this point, the subscriber has returned, and now we need to wait until
    // all outstanding write tasks complete
    executor.awaitTermination(999, TimeUnit.DAYS)
    logger.info(s"Sink has written ${adder.sum} rows")

    // we close all writers together so its atomic-ish
    logger.info("Closing all sink writers...")
    writers.foreach(_.close)
    logger.info("All sink writers are closed")

    if (failure != null)
      throw failure

    adder.sum()
  }
}