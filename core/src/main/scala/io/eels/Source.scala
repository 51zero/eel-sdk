package io.eels

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, TimeUnit, ArrayBlockingQueue, Executors}

import com.sksamuel.scalax.collection.BlockingQueueConcurrentIterator
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.concurrent.ExecutionContext
import scala.language.implicitConversions

trait Source extends StrictLogging {
  self =>

  def schema: FrameSchema
  def readers: Seq[Reader]

  def toFrame(ioThreads: Int): Frame = new Frame {

    override def buffer(ignored: Int)(implicit executor: ExecutionContext): Buffer = {
      import com.sksamuel.scalax.concurrent.ExecutorImplicits._

      val queue = new ArrayBlockingQueue[Row](1000)

      val executor = Executors.newFixedThreadPool(ioThreads)
      logger.debug(s"Source will read using $ioThreads io threads")

      val readers = self.readers
      logger.debug(s"Source has ${readers.size} reader(s)")

      val count = new AtomicLong(0)
      val latch = new CountDownLatch(readers.size)
      for ( reader <- readers ) {
        executor.submit {
          try {
            reader.iterator.foreach(queue.put)
            logger.debug(s"Completed reader #${count.incrementAndGet}")
            latch.countDown()
          } finally {
            reader.close()
          }
        }
      }
      executor.submit {
        latch.await(1, TimeUnit.DAYS)
        logger.debug("Readers completed; latch released")
        try {
          queue.put(Row.Sentinel)
        } catch {
          case e: Throwable =>
            logger.warn("Error adding sentinel", e)
        }
        logger.debug("Sentinel added to queue")
      }

      new Buffer {
        override def close(): Unit = executor.shutdownNow()
        override def iterator: Iterator[Row] = new Iterator[Row] {
          val iter = BlockingQueueConcurrentIterator(queue, Row.Sentinel)
          override def hasNext: Boolean = iter.hasNext
          override def next(): Row = iter.next()
        }
      }
    }

    override def schema: FrameSchema = self.schema
  }
}

/**
  * A one time usable reader of data.
  * Clients must call close() when terminating the reader, even if the end of the iterator has been reached.
  */
trait Reader {
  def close(): Unit
  def iterator: Iterator[Row]
}

object Source {
  implicit def toFrame(source: Source): Frame = source.toFrame(1)
}