package io.eels.datastream

import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
import io.eels.{Row, Sink}

case class SinkAction(ds: DataStream, sink: Sink, parallelism: Int) extends Logging {

  def execute(): Long = {

    val schema = ds.schema
    val adder = new LongAdder

    val latch = new CountDownLatch(parallelism)
    val queue = new LinkedBlockingQueue[Seq[Row]](1000)

    var failure: Throwable = null

    sink.open(schema, parallelism).zipWithIndex.foreach { case (stream, k) =>
      ExecutorInstances.io.submit(new Runnable {
        logger.debug(s"Starting output stream $k")
        override def run(): Unit = {
          try {
            BlockingQueueConcurrentIterator(queue, Row.Sentinel).foreach { chunk =>
              chunk.foreach { row =>
                stream.write(row)
                adder.increment()
              }
            }
          } catch {
            case t: Throwable =>
              logger.error("Error writing to stream", t)
              failure = t
          } finally {
            logger.debug(s"Closing output stream $k")
            stream.close()
            latch.countDown()
          }
        }
      })
    }

    ds.subscribe(new Subscriber[Seq[Row]] {
      override def starting(s: Cancellable): Unit = ()
      override def next(t: Seq[Row]): Unit = {
        queue.put(t)
      }
      override def completed(): Unit = {
        queue.put(Row.Sentinel)
      }
      override def error(t: Throwable): Unit = {
        queue.put(Row.Sentinel)
        throw t
      }
    })

    // at this point, the subscriber has returned, and now we need to wait until the
    // queue has been emptied by the io threads
    latch.await()
    logger.info(s"Sink has written ${adder.sum} rows")
    if (failure != null)
      throw failure
    adder.sum()
  }
}