package io.eels.plan

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import io.eels.Frame
import io.eels.Logging

object ToSizePlan : Plan(), Logging {

  fun apply(frame: Frame): Long {
    logger.info("Plan will execute with $tasks tasks")

    val buffer = frame.buffer()
    val latch = CountDownLatch(tasks)
    val running = AtomicBoolean(true)

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
    return futures.flatMap { it.value.get.toOption }.foldLeft(0L)(_ + _)
  }
}