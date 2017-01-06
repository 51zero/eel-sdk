package io.eels

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, LinkedBlockingQueue}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.concurrent.ExecutorImplicits._
import io.eels.schema.StructType

import scala.util.control.NonFatal

object SourceFrame {
  private val Poison = List(Row.Sentinel)
  private val executor = Executors.newCachedThreadPool()
}

class SourceFrame(source: Source, listener: Listener = NoopListener) extends Frame with Logging {

  override lazy val schema: StructType = source.schema

  def rows(): CloseableIterator[Row] = {

    val completed = new AtomicInteger(0)
    // by using a list of rows we reduce contention on the queue
    val queue = new LinkedBlockingQueue[Seq[Row]](100)
    val parts = source.parts()

    logger.debug(s"Submitting ${parts.size} parts to executor")
    parts.foreach { part =>
      SourceFrame.executor.submit {
        try {
          part.iterator().foreach { rows =>
            queue.put(rows)
            rows.foreach(listener.onNext)
          }
        } catch {
          case NonFatal(e) =>
            logger.error("Error while reading from source", e)
        }
        // once all the reading tasks are complete we need to indicate that we
        // are finished with the queue, so we add a sentinel for the reading thread to pick up
        // by using an atomic int, we know only one thread will get inside the condition
        if (completed.incrementAndGet == parts.size) {
          logger.debug("All parts completed; adding sentinel to shutdown queue")
          queue.put(SourceFrame.Poison)
        }
      }
    }

    new CloseableIterator[Row] {
      override def close(): Unit = {
        super.close()
        queue.put(SourceFrame.Poison)
      }
      override val iterator: Iterator[Row] =
        Iterator.continually(queue.take).takeWhile(_ != SourceFrame.Poison).flatten
    }
  }
}
