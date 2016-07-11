package io.eels.plan

import io.eels.Frame
import io.eels.Row
import io.eels.util.Logging
import java.util.concurrent.atomic.AtomicBoolean

object ToListPlan : Plan(), Logging {
  fun execute(frame: Frame): List<Row> {
    logger.info("Executing toList on frame [tasks=${tasks}]")

    val observable = frame.observable()
    //  val latch = CountDownLatch(tasks)
    val running = AtomicBoolean(true)

    val lists = try {
      val list = mutableListOf<Row>()
      observable.subscribe { list.add(it) }
      list
    } catch (e: Throwable) {
      logger.error("Error reading; aborting tasks", e)
      running.set(false)
      throw e
    } finally {
      // latch.countDown()
    }

    //   latch.await(timeout, TimeUnit.NANOSECONDS)
    logger.debug("Reading completed")

    //    raiseExceptionOnFailure(futures)

    //  val seqs = Await.result(Future.sequence(futures), 1.minute)
    //seqs.reduce((a, b) => a++b).map(internal => Row(schema, internal))
    return lists //.reduce { a, b -> a.plus(b) }
  }
}