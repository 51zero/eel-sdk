package io.eels

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, TimeUnit}

class SinkPlan(sink: Sink, frame: Frame) extends ConcurrentPlan[Long] {

  override def runConcurrent(concurrency: Int): Long = {

    import com.sksamuel.scalax.concurrent.ExecutorImplicits._

    val executor = Executors.newFixedThreadPool(concurrency)
    val count = new AtomicLong(0)
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
