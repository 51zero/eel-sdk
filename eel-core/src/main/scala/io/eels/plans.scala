package io.eels

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

class ExistsPlan(frame: Frame, p: (Row) => Boolean) extends Plan[Boolean] with Using {
  override def run: Boolean = {
    using(frame.buffer) { buffer =>
      buffer.iterator.exists(p)
    }
  }
}

class FindPlan(frame: Frame, p: (Row) => Boolean) extends Plan[Option[Row]] with Using {
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
    map.keySet.asScala.toSet
  }
}

class ToListPlan(frame: Frame) extends ConcurrentPlan[List[Row]] with Using with StrictLogging {

  import com.sksamuel.scalax.concurrent.ThreadImplicits.toRunnable
  import scala.collection.JavaConverters._

  override def runConcurrent(workers: Int): List[Row] = {
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
    queue.asScala.toList
  }
}

class ForallPlan(frame: Frame, p: Row => Boolean) extends Plan[Boolean] with Using {
  override def run: Boolean = {
    using(frame.buffer) { buffer =>
      buffer.iterator.forall(p)
    }
  }
}

class ToSizePlan(frame: Frame) extends Plan[Long] with Using {
  override def run: Long = {
    using(frame.buffer) { buffer =>
      buffer.iterator.size
    }
  }
}