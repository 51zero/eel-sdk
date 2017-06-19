package io.eels.datastream

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, LinkedBlockingQueue}

import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
import com.sksamuel.exts.concurrent.ExecutorImplicits._
import com.sksamuel.exts.config.ConfigResolver
import io.eels.schema.StructType
import io.eels.{CloseableIterator, Listener, NoopListener, Row, Source}

import scala.util.Try
import scala.util.control.NonFatal

// an implementation of DataStream that will read its partitions from a source
class DataStreamSource(source: Source, listener: Listener = NoopListener) extends DataStream {

  private val config = ConfigResolver()

  override def schema: StructType = source.schema
  override private[eels] def partitions = {

    val executor = Executors.newCachedThreadPool()

    // we buffer the reading from the sources so that slow io can constantly be performing in the background
    // by using a list of rows we reduce contention on the queue
    val parts = source.parts()

    // each part should be read in on an io-thread-pool
    logger.debug(s"Submitting ${parts.size} parts to executor")
    val partitions = source.parts().zipWithIndex.map { case (part, k) =>

      val queue = new LinkedBlockingQueue[Row](config.getInt("eel.source.defaultBufferSize"))
      val running = new AtomicBoolean(true)

      logger.info(s"Submitting partition ${k + 1} to executor...")
      executor.submit {
        try {
          logger.info(s"Starting partition ${k + 1}")
          val CloseableIterator(closeable, iterator) = part.iterator()
          try {
            iterator.takeWhile(_ => running.get).foreach { row =>
              queue.put(row)
              listener.onNext(row)
            }
            listener.onComplete()
          } catch {
            case NonFatal(e) =>
              logger.error("Error while reading from source", e)
              listener.onError(e)
          } finally {
            Try {
              closeable.close()
            }
            // we must put the sentinel so the downstream knows when the queue has finished
            queue.put(Row.Sentinel)
          }
        } catch {
          case t: Throwable => logger.error(s"Error starting partition thread ${k + 1}", t)
        }
      }

      CloseableIterator(new Closeable {
        override def close(): Unit = {
          logger.debug(s"Closing partition ${k + 1}")
          running.set(false)
        }
      }, BlockingQueueConcurrentIterator(queue, Row.Sentinel))
    }

    // the executor will shut down once all the partitions have completed
    executor.shutdown()

    partitions
  }
}
