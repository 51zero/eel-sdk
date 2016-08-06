package io.eels

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.sksamuel.exts.Logging
import rx.lang.scala.Subscriber

object SinkPlan extends Logging {

  def execute(sink: Sink, frame: Frame): Long = {

    val schema = frame.schema()
    val writer = sink.writer(schema)
    val count = new AtomicLong(0L)
    val latch = new CountDownLatch(1)

    frame.rows().subscribe(new Subscriber[Row]() {
      override def onError(e: Throwable) {
        logger.error("Error writing row", e)
      }

      override def onNext(row: Row) {
        if (row != null) {
          writer.write(row)
          count.incrementAndGet()
        }
      }

      override def onCompleted() {
        latch.countDown()
      }
    })

    latch.await(1, TimeUnit.DAYS)
    writer.close()

    count.get()
  }
}