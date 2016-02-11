package io.eels

import java.util.concurrent.{CountDownLatch, TimeUnit, Executors}
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.slf4j.StrictLogging
import com.sksamuel.scalax.concurrent.ExecutorImplicits._

class SinkPlan(sink: Sink, frame: Frame) extends ConcurrentPlan[Long] with StrictLogging {

  override def runConcurrent(workers: Int): Long = {

    val count = new AtomicLong(0)
    val latch = new CountDownLatch(workers)
    val buffer = frame.buffer
    val executors = Executors.newFixedThreadPool(workers)
    for ( k <- 1 to workers ) {
      executors.submit {
        val writer = sink.writer
        try {
          buffer.iterator.foreach { row =>
            writer.write(row)
            count.incrementAndGet()
          }
        } catch {
          case e: Throwable =>
            logger.error("Error writing; shutting down executor", e)
            executors.shutdownNow()
            throw e
        } finally {
          writer.close()
          logger.debug("Closed writer")
          latch.countDown()
        }
      }
    }
    executors.submit {
      latch.await(1, TimeUnit.DAYS)
      buffer.close()
      logger.debug("Closed buffer")
    }

    executors.shutdown()
    executors.awaitTermination(1, TimeUnit.HOURS)

    count.get()
  }
}
