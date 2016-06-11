package io.eels.util

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A fixed thread pool executor which will block on submits once the specified queue size has been reached.
 */
class BoundedThreadPoolExecutor(poolSize: Int, queueSize: Int) :
    ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, LinkedBlockingQueue<Runnable>()), AutoCloseable {

  val semaphore = Semaphore(poolSize + queueSize)
  val running = AtomicBoolean(true)

  override fun execute(runnable: Runnable): Unit {

    var acquired = false
    while (running.get() && !acquired) {
      try {
        semaphore.acquire()
        acquired = true
      } catch (e: InterruptedException) {
      }
    }

    try {
      super.execute(runnable)
    } catch (e: RejectedExecutionException) {
      semaphore.release()
      throw e
    }
  }

  override fun afterExecute(r: Runnable, t: Throwable): Unit {
    super.afterExecute(r, t)
    semaphore.release()
  }

  override fun close(): Unit {
    running.set(false)
  }
}