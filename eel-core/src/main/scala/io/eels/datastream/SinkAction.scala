package io.eels.datastream

import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
import io.eels.{Row, Sink}

case class SinkAction(ds: DataStream, sink: Sink, parallelism: Int) extends Logging {

  def execute(): Long = {

    val schema = ds.schema
    val total = new LongAdder

    val latch = new CountDownLatch(parallelism)
    val queue = new LinkedBlockingQueue[Seq[Row]](1000)

    sink.open(schema, parallelism).zipWithIndex.foreach { case (stream, k) =>
      ExecutorInstances.io.submit(new Runnable {
        logger.debug(s"Starting output stream $k")
        override def run(): Unit = {
          try {
            BlockingQueueConcurrentIterator(queue, Nil).foreach { chunk =>
              chunk.foreach(stream.write)
            }
          } catch {
            case t: Throwable => logger.error("Error writing out", t)
          } finally {
            logger.debug(s"Closing output stream $k")
            stream.close()
            latch.countDown()
          }
        }
      })
    }

    ds.subscribe(new Subscriber[Seq[Row]] {
      override def started(s: Cancellable): Unit = ()
      override def next(t: Seq[Row]): Unit = {
        queue.put(t)
      }
      override def completed(): Unit = queue.put(Nil)
      override def error(t: Throwable): Unit = {
        queue.put(Nil)
        throw t
      }
    })

    // at this point, the subscriber has returned, and now we need to wait until the
    // queue has been emptied by the io threads
    latch.await()
    total.sum()
  }
}