package io.eels.datastream

import java.io.Closeable
import java.util.concurrent.{Executors, LinkedBlockingQueue}

import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
import com.sksamuel.exts.concurrent.ExecutorImplicits._
import com.sksamuel.exts.config.ConfigResolver
import com.sksamuel.exts.io.Using
import io.eels.schema.StructType
import io.eels.{Channel, Listener, NoopListener, Row, Source}

import scala.util.control.NonFatal

// an implementation of DataStream that will read its partitions from a source
class DataStreamSource(source: Source, listener: Listener = NoopListener) extends DataStream with Using {

  private val config = ConfigResolver()

  override def schema: StructType = source.schema
  override private[eels] def channels = {

    // we buffer the reading from the sources so that slow io can constantly be performing in the background
    // by using a list of rows we reduce contention on the queue

    val io = Executors.newCachedThreadPool()

    val parts = source.parts()
    if (parts.isEmpty) {
      Nil
    } else {
      // each part should be read in on an io-thread-pool
      logger.debug(s"Preparing to read ${parts.size} parts")
      val partitions = parts.zipWithIndex.map { case (part, k) =>

        val queue = new LinkedBlockingQueue[Row](config.getInt("eel.source.defaultBufferSize"))

        logger.debug(s"Submitting source part ${k + 1} to executor...")
        io.submit {
          try {
            logger.debug(s"Starting source part ${k + 1}")
            using(part.channel) { case Channel(_, iterator) =>
              iterator.foreach { row =>
                queue.put(row)
                listener.onNext(row)
              }
              logger.debug(s"Source part ${k + 1} has completed")
              listener.onComplete()
            }
          } catch {
            case NonFatal(e) =>
              logger.error(s"Error in source part thread ${k + 1}", e)
              listener.onError(e)
            case e: InterruptedException =>
              logger.error(s"Source part ${k + 1} was interrupted")
              listener.onError(e)
          } finally {
            // we must put the sentinel so the downstream knows when the queue has finished
            queue.put(Row.Sentinel)
          }
        }

        Channel(new Closeable {
          override def close(): Unit = {
            logger.debug(s"Closing part ${k + 1}")
            io.shutdownNow()
          }
        }, BlockingQueueConcurrentIterator(queue, Row.Sentinel))
      }

      // the executor will shut down once all the partitions have completed
      io.shutdown()

      partitions
    }
  }
}
