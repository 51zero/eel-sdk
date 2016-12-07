package io.eels

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, LinkedBlockingQueue}
import java.util.function.Consumer

import com.sksamuel.exts.Logging
import com.sksamuel.exts.concurrent.ExecutorImplicits._
import io.eels.schema.StructType
import reactor.core.publisher.{Flux, FluxSink}

class SourceFrame(source: Source, listener: Listener = NoopListener, ioThreads: Int = 8) extends Frame with Logging {

  val RowListSentinel = List(Row.Sentinel)

  override lazy val schema: StructType = source.schema()

  def rows2(): Iterator[List[Row]] = {

    val completed = new AtomicInteger(0)
    val executor = Executors.newFixedThreadPool(ioThreads)
    val queue = new LinkedBlockingQueue[List[Row]](1000)
    val parts = source.parts()

    logger.debug(s"Submitting ${parts.size} parts to executor")
    parts.foreach { part =>
      executor.submit {
        part.iterator().foreach(queue.put)
        // once all the reading tasks are complete we need to indicate that we
        // are finished with the queue, so we add a sentinel for the reading thread to pick up
        // by using an atomic int, we know only one thread will get inside the condition
        if (completed.incrementAndGet == parts.size) {
          logger.debug("All parts completed; adding sentinel to shutdown queue")
          queue.put(RowListSentinel)
        }
      }
    }
    executor.shutdown()

    Iterator.continually(queue.take).takeWhile(_ != RowListSentinel)
  }

  override def rows(): Flux[Row] = Flux.create(new Consumer[FluxSink[Row]] {
    override def accept(sink: FluxSink[Row]): Unit = {

      val parts = source.parts()
      if (parts.isEmpty) {
        Flux.empty()
      } else {
        val queue = new LinkedBlockingQueue[List[Row]](100)
        val completed = new AtomicInteger(0)

        val executor = Executors.newFixedThreadPool(ioThreads)
        logger.debug(s"Submitting ${parts.size} parts to executor")
        parts.foreach { part =>
          executor.submit {
            part.iterator().takeWhile(_ => !sink.isCancelled).foreach { rows =>
              queue.put(rows)
              rows.foreach(listener.onNext)
            }
            // once all the reading tasks are complete we need to indicate that we
            // are finished with the queue, so we add a sentinel for the reading thread to pick up
            // by using an atomic int, we know only one thread will get inside the condition
            if (completed.incrementAndGet == parts.size) {
              logger.debug("All parts completed; adding sentinel to close queue")
              queue.put(RowListSentinel)
              listener.onComplete()
            }
          }
        }
        executor.shutdown()

        Iterator.continually(queue.take)
          .takeWhile(_ != RowListSentinel)
          .foreach { rows =>
            rows.foreach(sink.next)
          }

        sink.complete()

      }
    }
  })
}
