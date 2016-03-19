package io.eels.plan

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.sksamuel.scalax.io.Using
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

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

    val futures = (1 to tasks).map { k =>
      Future {
        try {
          val list = ListBuffer[InternalRow]()
          buffer.iterator.takeWhile(_ => running.get).foreach(r => list += r)
          list
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

    val seqs = Await.result(Future.sequence(futures), 1.minute)
    seqs.reduce((a, b) => a ++ b).map(internal => Row(schema, internal))
  }
}
