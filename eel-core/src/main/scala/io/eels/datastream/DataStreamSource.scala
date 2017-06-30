package io.eels.datastream

import java.io.Closeable
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, LinkedBlockingQueue}

import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
import com.sksamuel.exts.concurrent.ExecutorImplicits._
import com.sksamuel.exts.config.ConfigResolver
import com.sksamuel.exts.io.Using
import io.eels.schema.StructType
import io.eels.{Flow, Listener, NoopListener, Row, Source}

import scala.util.control.NonFatal

// an implementation of DataStream that provides a single flow populated from 1 or more parts
class DataStreamSource(source: Source, listener: Listener = NoopListener) extends DataStream with Using {

  private val config = ConfigResolver()

  override def schema: StructType = source.schema
  override private[eels] def flows = {

    val parts = source.parts()
    if (parts.isEmpty) {
      Nil
    } else {

      // we buffer the reading from the sources so that slow io can constantly be performing in the background
      val io = Executors.newCachedThreadPool()

      val queue = new LinkedBlockingQueue[Row](config.getInt("eel.source.defaultBufferSize"))
      val completed = new AtomicLong(0)

      // each part should be read in an io-thread-pool writing to a shared queue
      logger.debug(s"Preparing to read ${parts.size} parts")
      parts.zipWithIndex.map { case (part, k) =>

        logger.debug(s"Submitting source part ${k + 1} to executor...")
        io.submit {
          try {
            logger.debug(s"Starting source part ${k + 1}")
            // the channel will always be closed when completed/failed by the using block
            using(part.open) { channel =>
              channel.iterator.foreach { row =>
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
            // when the final part has finished, we must put the sentinel onto the queue so
            // downstream knows the reading has finished
            if (completed.incrementAndGet == parts.size) {
              queue.put(Row.Sentinel)
            }
          }
        }
      }

      // the executor will shut down once all the partitions have completed
      io.shutdown()

      // if requested to shutdown this flow, then we'll interupt the worker threads
      val closeable = new Closeable {
        override def close(): Unit = io.shutdownNow()
      }

      val iterator = BlockingQueueConcurrentIterator(queue, Row.Sentinel)
      Seq(Flow(closeable, iterator))
    }
  }
}
