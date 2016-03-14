package io.eels.plan

import java.util.concurrent.atomic.{AtomicBoolean, LongAdder}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Frame, Sink}

import scala.concurrent.{ExecutionContext, Future}

object SinkPlan extends Plan with StrictLogging {

  def apply(sink: Sink, frame: Frame)(implicit execution: ExecutionContext): Long = {
    val latch = new CountDownLatch(tasks)
    val schema = frame.schema
    val buffer = frame.buffer
    val writer = sink.writer(schema)
    val running = new AtomicBoolean(true)

    val futures = for (k <- 1 to tasks) yield {
      Future {
        try {
          buffer.iterator.takeWhile(_ => running.get).foldLeft(0L) { case (total, row) =>
            writer.write(row)
            total + 1
          }
        } catch {
          case e: Throwable =>
            logger.error("Error writing; shutting down executor", e)
            running.set(false)
            throw e
        } finally {
          latch.countDown()
        }
      }
    }

    logger.debug(s"Waiting ${timeout.toMillis}ms for sink to complete")
    latch.await(timeout.toNanos, TimeUnit.NANOSECONDS)
    logger.debug("Closing buffer")
    writer.close()
    buffer.close()
    logger.debug("Buffer closed")

    raiseExceptionOnFailure(futures)
    futures.flatMap(f => f.value.get.toOption).foldLeft(0L)(_ + _)
  }
}
