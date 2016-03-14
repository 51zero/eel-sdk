package io.eels.plan

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}

import com.sksamuel.scalax.io.Using
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels._

import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

object ToSeqPlan extends Plan with Using with StrictLogging {

  def typed[T](frame: Frame)(implicit executor: ExecutionContext, manifest: Manifest[T]): Seq[T] = {
    val constructor = manifest.runtimeClass.getConstructors.head
    untyped(frame).map { row =>
      constructor.newInstance(row.values.asInstanceOf[Seq[Object]]: _*).asInstanceOf[T]
    }
  }

  def untyped(frame: Frame)(implicit executor: ExecutionContext): Seq[Row] = {
    logger.info(s"Executing toSeq on frame [tasks=$tasks]")

    val buffer = frame.buffer
    val schema = frame.schema
    val latch = new CountDownLatch(tasks)
    val running = new AtomicBoolean(true)

    logger.info(s"Plan will execute with $tasks tasks")
    val futures = (1 to tasks).map { case k =>
      Future {
        try {
          val list = ListBuffer[InternalRow]()
          buffer.iterator.takeWhile(_ => running.get).foreach(r => list += r)
          list
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

    raiseExceptionOnFailure(futures)
    val result = futures.flatMap(f => f.value.get.toOption)
      .withFilter(_.nonEmpty)
      .flatMap(rows => rows)
      .map(internal => Row(schema, internal))
    result.toVector
  }
}
