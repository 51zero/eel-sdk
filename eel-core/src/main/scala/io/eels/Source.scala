package io.eels

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent._

import com.sksamuel.scalax.Logging
import com.sksamuel.scalax.collection.BlockingQueueConcurrentIterator
import com.sksamuel.scalax.io.Using
import com.typesafe.config.ConfigFactory

import scala.language.implicitConversions

trait Source extends Logging {
  self =>

  def schema: Schema
  def parts: Seq[Part]

  def toFrame(ioThreads: Int): Frame = new FrameSource(ioThreads, this)
}

class FrameSource(ioThreads: Int, source: Source) extends Frame with Logging with Using {

  private val config = ConfigFactory.load()
  private val bufferSize = config.getInt("eel.source.defaultBufferSize")
  logger.debug(s"FrameSource is configured with bufferSize=$bufferSize")

  override lazy val schema: Schema = source.schema

  override def buffer: Buffer = {
    import com.sksamuel.scalax.concurrent.ThreadImplicits.toRunnable

    val executor = Executors.newFixedThreadPool(ioThreads)
    logger.debug(s"Source will read using a FixedThreadPool [ioThreads=$ioThreads]")

    val parts = source.parts
    logger.debug(s"Source has ${parts.size} parts")

    // faster to use a linked blocking queue than an array one
    // because a linkedbq has a lock for both head and tail so allows us to push and pop at same time
    // very useful for our pipeline
    val queue = new LinkedBlockingQueue[InternalRow](bufferSize)
    val count = new AtomicLong(0)
    val latch = new CountDownLatch(parts.size)

    for (part <- parts) {
      executor.submit {
        try {
          using(part.reader) { reader =>
            reader.iterator.foreach(queue.put)
            logger.debug(s"Completed part #${count.incrementAndGet}")
          }
        } finally {
          latch.countDown()
        }
      }
    }

    executor.submit {
      latch.await(1, TimeUnit.DAYS)
      logger.debug("Source parts completed; latch released")
      try {
        queue.put(InternalRow.PoisonPill)
        logger.debug("PoisonPill added to downstream queue from source")
      } catch {
        case e: Throwable =>
          logger.error("Error adding PoisonPill", e)
      }
    }

    executor.shutdown()

    new Buffer {
      override def close(): Unit = executor.shutdownNow()
      override def iterator: Iterator[InternalRow] = BlockingQueueConcurrentIterator(queue, InternalRow.PoisonPill)
    }
  }
}

/**
  *
  * A Part represents part of the source data. Eg a single path in a multifile source, or a single table
  * in a multitable source. A part provides a reader for that source when requested.
  */
trait Part {
  def reader: SourceReader
}

/**
  * A one time usable reader of data.
  * Clients must call close() when terminating the reader, even if the end of the iterator has been reached.
  */
trait SourceReader {
  def close(): Unit
  def iterator: Iterator[InternalRow]
}

object Source {
  implicit def toFrame(source: Source): Frame = source.toFrame(1)
}