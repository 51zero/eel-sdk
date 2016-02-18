package io.eels

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, CountDownLatch, Executors, TimeUnit}

import com.sksamuel.scalax.io.Using
import com.typesafe.scalalogging.slf4j.StrictLogging

class HeadPlan(frame: Frame) extends Plan[Option[Row]] with Using {
  override def run: Option[Row] = {
    using(frame.buffer) { buffer =>
      buffer.iterator.take(1).toList.headOption
    }
  }
}

class ExistsPlan(frame: Frame, p: Row => Boolean) extends Plan[Boolean] with Using {
  override def run: Boolean = {
    using(frame.buffer) { buffer =>
      buffer.iterator.exists(p)
    }
  }
}

class FindPlan(frame: Frame, p: Row => Boolean) extends Plan[Option[Row]] with Using {
  override def run: Option[Row] = {
    using(frame.buffer) { buffer =>
      buffer.iterator.find(p)
    }
  }
}

class ToSetPlan(frame: Frame) extends ConcurrentPlan[Set[Row]] with Using with StrictLogging {

  import com.sksamuel.scalax.concurrent.ThreadImplicits.toRunnable
  import scala.collection.JavaConverters._

  override def runConcurrent(workers: Int): Set[Row] = {
    val map = new ConcurrentHashMap[Row, Boolean]
    val buffer = frame.buffer
    val latch = new CountDownLatch(workers)
    val executor = Executors.newFixedThreadPool(workers)
    for ( k <- 1 to workers ) {
      executor.submit {
        try {
          buffer.iterator.foreach(map.putIfAbsent(_, true))
        } catch {
          case e: Throwable =>
            logger.error("Error writing; shutting down executor", e)
            executor.shutdownNow()
            throw e
        } finally {
          latch.countDown()
        }
      }
    }
    executor.submit {
      latch.await(1, TimeUnit.DAYS)
      logger.debug("Closing buffer")
      buffer.close()
      logger.debug("Buffer closed")
    }
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.DAYS)
    logger.debug("Set data is complete into map; building set")
    map.keySet.asScala.toSet
  }
}

class ToSeqPlan(frame: Frame) extends ConcurrentPlan[Seq[Row]] with Using with StrictLogging {

  import com.sksamuel.scalax.concurrent.ThreadImplicits.toRunnable
  import scala.collection.JavaConverters._

  override def runConcurrent(workers: Int): Seq[Row] = {
    val queue = new ConcurrentLinkedQueue[Row]
    val buffer = frame.buffer
    val latch = new CountDownLatch(workers)
    val executor = Executors.newFixedThreadPool(workers)
    for ( k <- 1 to workers ) {
      executor.submit {
        try {
          buffer.iterator.foreach(queue.add)
        } catch {
          case e: Throwable =>
            logger.error("Error writing; shutting down executor", e)
            executor.shutdownNow()
            throw e
        } finally {
          latch.countDown()
        }
      }
    }
    executor.submit {
      latch.await(1, TimeUnit.DAYS)
      logger.debug("Closing buffer")
      buffer.close()
      logger.debug("Buffer closed")
    }
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.DAYS)
    queue.asScala.toVector
  }
}

class ForallPlan(frame: Frame, p: Row => Boolean) extends Plan[Boolean] with Using {
  override def run: Boolean = {
    using(frame.buffer) { buffer =>
      buffer.iterator.forall(p)
    }
  }
}

class ToSizePlan(frame: Frame) extends ConcurrentPlan[Long] with StrictLogging {

  import com.sksamuel.scalax.concurrent.ThreadImplicits.toRunnable

  override def runConcurrent(workers: Int): Long = {

    val count = new AtomicLong(0)
    val buffer = frame.buffer
    val latch = new CountDownLatch(workers)
    val executor = Executors.newFixedThreadPool(workers)
    for ( k <- 1 to workers ) {
      executor.submit {
        try {
          buffer.iterator.foreach(_ => count.incrementAndGet)
        } catch {
          case e: Throwable =>
            logger.error("Error writing; shutting down executor", e)
            executor.shutdownNow()
            throw e
        } finally {
          latch.countDown()
        }
      }
    }
    executor.submit {
      latch.await(1, TimeUnit.DAYS)
      logger.debug("Closing buffer")
      buffer.close()
      logger.debug("Buffer closed")
    }
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.DAYS)
    count.get()
  }
}