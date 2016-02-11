package io.eels

import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.concurrent.ExecutionContext

class SinkPlan(sink: Sink, frame: Frame) extends ConcurrentPlan[Long] with StrictLogging {

  override def runConcurrent(workers: Int)(implicit executor: ExecutionContext): Long = {

    val count = new AtomicLong(0)
    val buffer = frame.buffer(workers)
    val writer = sink.writer
    buffer.iterator.foreach { row =>
      writer.write(row)
      count.incrementAndGet()
    }
    writer.close()
    logger.debug("Closed writer")
    buffer.close()
    logger.debug("Closed buffer")
    count.get()
  }
}
