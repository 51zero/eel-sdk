package io.eels.datastream

import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{CountDownLatch, Executors, LinkedBlockingQueue, TimeUnit}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
import com.sksamuel.exts.concurrent.ExecutorImplicits._
import io.eels.{Channel, Listener, NoopListener, Row, Sink}

import scala.util.Try
import scala.util.control.NonFatal

case class SinkAction(ds: DataStream, sink: Sink) extends Logging {

  def execute(listener: Listener = NoopListener): Long = {

    val schema = ds.schema
    val channels = ds.channels
    val total = new LongAdder
    val latch = new CountDownLatch(channels.size)

    // each output stream will operate in an io thread.
    val io = Executors.newCachedThreadPool()

    // we open up a seperate output stream for each partition
    val streams = sink.open(schema, channels.size)

    channels.zip(streams).zipWithIndex.foreach { case ((Channel(_, iterator), stream), k) =>
      logger.debug(s"Starting writing task ${k + 1}")

      val buffer = new LinkedBlockingQueue[Row](100)

      ds.executor.execute(new Runnable {
        override def run(): Unit = {
          // each channel has its own output stream, so once the iterator is finished we must notify the buffer
          // so the output stream can be closed
          try {
            iterator.foreach { row =>
              listener.onNext(row)
              buffer.put(row)
            }
            listener.onComplete()
          } catch {
            case NonFatal(e) =>
              logger.error("Error populating write buffer", e)
              listener.onError(e)
          } finally {
            logger.debug(s"Writing task ${k + 1} has completed")
            buffer.put(Row.Sentinel)
          }
        }
      })

      // each channel will have an io-thread which will write to the file/disk
      io.submit {

        val localCount = new LongAdder

        try {
          BlockingQueueConcurrentIterator(buffer, Row.Sentinel).foreach { row =>
            stream.write(row)
            localCount.increment()
          }
          logger.info(s"Channel ${k + 1} has completed; wrote ${localCount.sum} records; closing writer")
        } catch {
          case NonFatal(e) =>
            logger.info(s"Channel ${k + 1} has errored; wrote ${localCount.sum} records; closing writer", e)
        } finally {
          Try {
            stream.close()
          }
          total.add(localCount.sum)
          latch.countDown()
        }
      }
    }

    io.shutdown()
    latch.await(21, TimeUnit.DAYS)
    logger.debug("Sink has completed; closing all input channels")
    channels.foreach { it => Try {
      it.close()
    }
    }
    total.sum()
  }
}
