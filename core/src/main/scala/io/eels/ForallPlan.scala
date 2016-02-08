package io.eels

import java.util.concurrent.{TimeUnit, Executors}
import java.util.concurrent.atomic.AtomicBoolean

class ForallPlan(frame: Frame, p: Row => Boolean) extends ConcurrentPlan[Boolean] {

  override def runConcurrent(concurrency: Int): Boolean = {

    import com.sksamuel.scalax.concurrent.ExecutorImplicits._

    val executor = Executors.newFixedThreadPool(concurrency)
    val forall = new AtomicBoolean(true)
    frame.parts.foreach { part =>
      executor.submit {
        if (!part.iterator.forall(p))
          forall.set(false)
      }
    }
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.DAYS)
    forall.get()
  }
}
