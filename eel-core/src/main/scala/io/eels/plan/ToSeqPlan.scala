package io.eels.plan

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import com.sksamuel.scalax.io.Using
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels._

import scala.collection.JavaConverters._
import scala.concurrent.{Future, ExecutionContext}

object ToSeqPlan extends Using with StrictLogging {

  val config = ConfigFactory.load()
  val slices = config.getInt("eel.tasks")

  def apply(frame: Frame)(implicit executor: ExecutionContext): Seq[Row] = {
    logger.info(s"Executing toSeq on frame [tasks=$slices]")

    val queue = new ConcurrentLinkedQueue[Row]
    val buffer = frame.buffer
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

    latch.await(1, TimeUnit.DAYS)
    logger.debug("Closing buffer")
    buffer.close()
    logger.debug("Buffer closed")

    queue.asScala.toVector
  }
}
