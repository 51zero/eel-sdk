package io.eels

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{Executors, TimeUnit}

trait Plan[T] {
  def run: T
}

trait ConcurrentPlan[T] extends Plan[T] {
  def run: T = run(1)
  def run(concurrency: Int): T
}

class SinkPlan(sink: Sink, frame: Frame) extends ConcurrentPlan[Int] {

  import com.sksamuel.scalax.concurrent.ExecutorImplicits._

  override def run(concurrency: Int): Int = {
    val executor = Executors.newFixedThreadPool(concurrency)
    val count = new AtomicInteger(0)
    frame.parts.foreach { part =>
      executor submit {
        val writer = sink.writer
        part.iterator.foreach { row =>
          writer.write(row)
          count.incrementAndGet()
        }
        writer.close()
      }
    }
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.DAYS)
    count.get()
  }
}

class ForallPlan(frame: Frame, p: Row => Boolean) extends ConcurrentPlan[Boolean] {
  override def run(concurrency: Int): Boolean = {
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