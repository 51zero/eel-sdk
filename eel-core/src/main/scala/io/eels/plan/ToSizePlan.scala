package io.eels.plan

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.Frame

import scala.concurrent.{ExecutionContext, Future}

object ToSizePlan extends Plan with StrictLogging {

  def apply(frame: Frame)(implicit executor: ExecutionContext): Long = {

    val buffer = frame.buffer
    val latch = new CountDownLatch(tasks)
    val running = new AtomicBoolean(true)

    logger.info(s"Plan will execute with $tasks tasks")
    val futures = for (k <- 1 to tasks) yield {
      Future {
        try {
          var count = 0
          buffer.iterator.takeWhile(_ => running.get).foreach { row =>
            count = count + 1
          }
          logger.debug(s"Task $k completed")
          count
        } catch {
          case e: Throwable =>
            logger.error("Error reading; aborting tasks", e)
            running.set(false)
            throw e
        } finally {
          latch.countDown()
        }
      }
    }

    latch.await(timeout.toNanos, TimeUnit.NANOSECONDS)
    logger.debug("Closing buffer")
    buffer.close()
    logger.debug("Buffer closed")

    raiseExceptionOnFailure(futures)
    futures.flatMap(f => f.value.get.toOption).foldLeft(0L)(_ + _)
  }
}
