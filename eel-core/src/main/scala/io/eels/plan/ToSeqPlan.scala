package io.eels.plan

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import com.sksamuel.scalax.io.Using
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

object ToSeqPlan extends Plan with Using with StrictLogging {

  def apply(frame: Frame)(implicit executor: ExecutionContext): Seq[Row] = {
    logger.info(s"Executing toSeq on frame [tasks=$slices]")

    val queue = new ConcurrentLinkedQueue[InternalRow]
    val buffer = frame.buffer
    val schema = frame.schema
    val latch = new CountDownLatch(slices)
    val running = new AtomicBoolean(true)

    for (k <- 1 to slices) {
      Future {
        try {
          buffer.iterator.takeWhile(_ => running.get).foreach(queue.add)
        } catch {
          case e: Throwable =>
            logger.error("Error writing; aborting tasks", e)
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

    queue.asScala.map(internal => Row(schema, internal)).toVector
  }
}
