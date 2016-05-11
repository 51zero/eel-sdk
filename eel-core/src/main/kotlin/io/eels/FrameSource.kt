package io.eels

import com.typesafe.config.ConfigFactory
import io.eels.component.Using
import one.util.streamex.StreamEx
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.stream.Stream

class FrameSource(val ioThreads: Int, val source: Source) : Frame, Logging, Using {

  private val config = ConfigFactory.load()
  private val bufferSize by lazy {
    val bufferSize = config.getInt("eel.source.defaultBufferSize")
    logger.debug("FrameSource is configured with bufferSize=$bufferSize")
    bufferSize
  }

  override fun schema(): Schema = source.schema()

  // this stream method may be invoked multiple times, each time generating a new "load action" and a new
  // resultant stream from it. I need to see whether this is the behaviour I want.
  override fun stream(): Stream<Row> {
    val executor = Executors.newFixedThreadPool(ioThreads)
    logger.debug("Source will read using a FixedThreadPool [ioThreads=$ioThreads]")

    val parts = source.parts()
    logger.debug("Source has ${parts.size} parts")

    // faster to use a linked blocking queue than an array one
    // because a linkedbq has a lock for both head and tail so allows us to push and pop at same time
    // very useful for our pipeline
    val queue = LinkedBlockingQueue<Row>(bufferSize)
    val count = AtomicLong(0)
    val latch = CountDownLatch(parts.size)

    for (part in parts) {
      executor.submit {
        try {
          using(part.reader()) {
            it.iterator().forEach { queue.put(it) }
            logger.debug("Completed part #${count.incrementAndGet()}")
          }
        } catch (e: Exception) {
          logger.error("Error while loading from source; this reader thread will quit", e)
        } finally {
          latch.countDown()
        }
      }
    }

    executor.submit {
      latch.await(1, TimeUnit.DAYS)
      logger.debug("Source parts completed; latch released")
      try {
        queue.put(Row.PoisonPill)
        logger.debug("PoisonPill added to downstream queue from source")
      } catch (e: Exception) {
        logger.error("Error adding PoisonPill", e)
      }
    }
    executor.shutdown()
    return StreamEx.generate { queue.take() }.takeWhile { it != Row.PoisonPill }
  }
}