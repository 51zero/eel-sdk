package io.eels.datastream

import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.concurrent.ExecutorImplicits._
import io.eels.{Listener, NoopListener, Sink}

import scala.util.Try
import scala.util.control.NonFatal

case class SinkAction(ds: DataStream, sink: Sink) extends Logging {

  def execute(listener: Listener = NoopListener): Long = {

    val schema = ds.schema
    val flows = ds.flows
    val total = new LongAdder
    val latch = new CountDownLatch(flows.size)

    // each output stream will operate in an io thread.
    val io = Executors.newCachedThreadPool()

    // we open up a seperate output stream for each flow
    val streams = sink.open(schema, flows.size)

    flows.zip(streams).zipWithIndex.foreach { case ((flow, stream), k) =>
      logger.debug(s"Starting writing task ${k + 1}")

      // each channel will have an io-thread which will write to the file/disk
      io.submit {

        val localCount = new LongAdder

        // each flow has its own output stream, so once the iterator is finished we
        // must should close the output stream immediately, no sense in keeping it open
        try {
          flow.iterator.foreach { row =>
            localCount.increment()
            stream.write(row)
            listener.onNext(row)
          }
          logger.info(s"Channel ${k + 1} has completed; wrote ${localCount.sum} records; closing output stream")
          listener.onComplete()
        } catch {
          case NonFatal(e) =>
            logger.info(s"Channel ${k + 1} has errored; wrote ${localCount.sum} records; closing output stream", e)
            listener.onError(e)
        } finally {
          Try { stream.close() }
          total.add(localCount.sum)
          latch.countDown()
        }
      }
    }

    io.shutdown()
    latch.await(999, TimeUnit.DAYS)

    logger.debug("Sink has completed; closing all input channels")
    flows.foreach { it => it.close() }
    total.sum()
  }
}
