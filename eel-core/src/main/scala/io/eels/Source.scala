package io.eels

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch, Executors, TimeUnit}

import com.sksamuel.scalax.collection.BlockingQueueConcurrentIterator
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.language.implicitConversions

trait Source extends StrictLogging {
  self =>

  val DefaultBufferSize = 1000

  def schema: FrameSchema
  def readers: Seq[Reader]

  def toFrame(ioThreads: Int): Frame = new Frame {

    override def schema: FrameSchema = self.schema

    override def buffer: Buffer = {
      import com.sksamuel.scalax.concurrent.ExecutorImplicits._

      val executor = Executors.newFixedThreadPool(ioThreads)
      logger.debug(s"Source will read using $ioThreads io threads")

      try {

        val readers = self.readers
        logger.debug(s"Source has ${readers.size} reader(s)")

        val queue = new ArrayBlockingQueue[InternalRow](DefaultBufferSize)
        val count = new AtomicLong(0)
        val latch = new CountDownLatch(readers.size)

        for ( reader <- readers ) {
          executor.submit {
            try {
              reader.iterator.foreach(queue.put)
              logger.debug(s"Completed reader #${count.incrementAndGet}")
            } catch {
              case e: Throwable =>
                logger.error("Error reading row; aborting", e)
            } finally {
              latch.countDown()
              reader.close()
            }
          }
        }

        executor.submit {
          latch.await(1, TimeUnit.DAYS)
          logger.debug("Readers completed; latch released")
          try {
            queue.put(InternalRow.Sentinel)
          } catch {
            case e: Throwable =>
              logger.error("Error adding sentinel", e)
          }
          logger.debug("Sentinel added to downstream queue from source")
        }

        executor.shutdown()

        new Buffer {
          override def close(): Unit = {
            logger.debug("Closing source")
            executor.shutdownNow()
          }
          override def iterator: Iterator[InternalRow] = BlockingQueueConcurrentIterator(queue, InternalRow.Sentinel)
        }

      } catch {
        case t: Throwable =>
          logger.error("Error opening readers", t)
          executor.shutdownNow()
          throw t
      }
    }
  }
}

/**
  *
  * A Part represents part of the source data. Eg a single path in a multifile source, or a single table
  * in a multitable source. A part provides a reader when requested.
  */
trait Part {
  def reader: Reader
}

/**
  * A one time usable reader of data.
  * Clients must call close() when terminating the reader, even if the end of the iterator has been reached.
  */
trait Reader {
  def close(): Unit
  def iterator: Iterator[InternalRow]
}

object Source {
  implicit def toFrame(source: Source): Frame = source.toFrame(1)
}