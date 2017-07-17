package io.eels.datastream

import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, TimeUnit}

import com.sksamuel.exts.Logging
import io.eels.{Row, SinkWriter, Sink}

import scala.collection.JavaConverters._

case class SinkAction(ds: DataStream, sink: Sink, parallelism: Int) extends Logging {

  def execute(): Long = {

    val schema = ds.schema
    val adder = new LongAdder

    var failure: Throwable = null

    val writers = new ConcurrentLinkedQueue[SinkWriter](sink.open(schema, parallelism).asJava)
    val executor = Executors.newFixedThreadPool(parallelism)

    class WriteTask(chunk: Seq[Row], k: Int) extends Runnable {
      override def run(): Unit = {
        // this cannot return null, as we created enough writers for everyone
        val writer = writers.poll()
        if (writer == null)
          logger.error("Bug: Writer was null")
        try {
          chunk.foreach { row =>
            writer.write(row)
            adder.increment()
          }
        } catch {
          case t: Throwable =>
            logger.error("Error writing to stream", t)
            // if we have an error writing, we'll exit immediately
            executor.shutdownNow()
            failure = t
        } finally {
          logger.debug(s"Chunk $k has completed")
          writers.add(writer)
        }
      }
    }

    ds.subscribe(new Subscriber[Seq[Row]] {
      var cancellable: Cancellable = null
      var count = 1
      override def starting(c: Cancellable): Unit = {
        logger.debug(s"Sink writer is starting [cancellable=$c]")
        this.cancellable = c
      }
      override def next(chunk: Seq[Row]): Unit = {
        // each time our subscriber gets a chunk it can queue a task to process that chunk
        // avoiding the need for a blocking queue
        executor.submit(new WriteTask(chunk, count))
        count = count + 1
      }
      override def completed(): Unit = {
        logger.debug("Sink action has received all chunks")
        executor.shutdown()
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
    writers.asScala.foreach(_.close)
    logger.info("All sink writers are closed")

    if (failure != null)
      throw failure

    adder.sum()
  }
}