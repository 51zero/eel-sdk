package io.eels.plan

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}

import io.eels.{Frame, Sink}

object SinkPlan extends Plan with StrictLogging {

  def apply(sink: Sink, frame: Frame)(implicit execution: ExecutionContext): Long = {

    val latch = new CountDownLatch(tasks)
    val schema = frame.schema
    val buffer = frame.buffer
    val writer = sink.writer(schema)
    val running = new AtomicBoolean(true)

    logger.info(s"Plan will execute with $tasks tasks")
    val futures = for (k <- 1 to tasks) yield {
      Future {
        var count = 0l
        try {
          buffer.iterator.takeWhile(_ => running.get).foreach { row =>
            writer.write(row)
            count = count + 1
          }
          count
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

    val counts = Await.result(Future.sequence(futures.map(_.recover { case _ => 0l })), 1.minute)
    counts.sum
  }
}
