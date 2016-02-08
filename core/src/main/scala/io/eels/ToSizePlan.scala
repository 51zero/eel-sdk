package io.eels

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, TimeUnit}

class ToSizePlan(frame: Frame) extends ConcurrentPlan[Long] {

  override def runConcurrent(concurrency: Int): Long = {

    val count = new AtomicLong(0)
    import com.sksamuel.scalax.concurrent.ExecutorImplicits._

    val executor = Executors.newFixedThreadPool(concurrency)

    frame.parts.foreach { part =>
      executor submit {
        count addAndGet part.iterator.size
      }
    }

    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.DAYS)

    count.get()
  }
}