package io.eels.plan

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean

import com.sksamuel.scalax.io.Using
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels._

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object ToSetPlan extends Plan with Using with StrictLogging {

  def typed[T](frame: Frame)(implicit executor: ExecutionContext, manifest: Manifest[T]): scala.collection.mutable.Set[T] = {
    val constructor = manifest.runtimeClass.getConstructors.head
    apply(frame).map { row =>
      constructor.newInstance(row.values.asInstanceOf[Seq[Object]]: _*).asInstanceOf[T]
    }
  }

  def apply(frame: Frame)(implicit executor: ExecutionContext): scala.collection.mutable.Set[Row] = {
    logger.info(s"Executing toSet on frame [tasks=$tasks]")

    val buffer = frame.buffer
    val schema = frame.schema
    val latch = new CountDownLatch(tasks)
    val running = new AtomicBoolean(true)

    logger.info(s"Plan will execute with $tasks tasks")
    val futures: Seq[Future[mutable.Set[InternalRow]]] = (1 to tasks).map { k =>
      Future {
        try {
          val map = mutable.Set[InternalRow]()
          buffer.iterator.takeWhile(_ => running.get).foreach(map.add)
          map
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

    val sets = Await.result(Future.sequence(futures), 1.minute)
    sets.reduce((a, b) => a ++ b).map(internal => Row(schema, internal))
  }
}
