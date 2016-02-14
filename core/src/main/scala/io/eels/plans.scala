package io.eels

import java.util.concurrent.{CountDownLatch, TimeUnit, Executors, LinkedBlockingQueue}

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

class ToListPlan(frame: Frame) extends ConcurrentPlan[List[Row]] with Using with StrictLogging {

  import com.sksamuel.scalax.concurrent.ThreadImplicits.toRunnable

  override def runConcurrent(workers: Int): List[Row] = {
    val queue = new LinkedBlockingQueue[Row]
    val buffer = frame.buffer
    val latch = new CountDownLatch(workers)
    val executor = Executors.newFixedThreadPool(workers)
    for ( k <- 1 to workers ) {
      executor.submit {
        try {
          buffer.iterator.foreach(queue.put)
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
      logger.debug("Closing buffer feed")
      buffer.close()
      logger.debug("Buffer feed closed")
    }
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.DAYS)
    import scala.collection.JavaConverters._
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