package io.eels.plan

import io.eels.Frame
import io.eels.Row
import io.eels.Sink
import io.eels.util.Logging
import rx.Subscriber
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

object SinkPlan : Logging {

  fun execute(sink: Sink, frame: Frame): Long {

    val schema = frame.schema()
    val writer = sink.writer(schema)
    val count = AtomicLong(0L)
    val latch = CountDownLatch(1)

    frame.rows().subscribe(object : Subscriber<Row>() {
      override fun onError(e: Throwable?) {
        logger.warn("Error writing row", e)
      }

      override fun onNext(row: Row?) {
        if (row != null) {
          writer.write(row)
          count.incrementAndGet()
        }
      }

      override fun onCompleted() {
        latch.countDown()
      }
    })

    latch.await(1, TimeUnit.DAYS)
    writer.close()

    return count.get()
  }
}