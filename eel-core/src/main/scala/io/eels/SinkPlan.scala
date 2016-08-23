package io.eels

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.sksamuel.exts.Logging
import rx.lang.scala.{Observer, Subscriber}

object SinkPlan extends Logging {

  def execute(sink: Sink, frame: Frame, observer: Observer[Row]): Long = {

    val schema = frame.schema()
    val writer = sink.writer(schema)
    val count = new AtomicLong(0L)

    // the latch is just to make this execute method blocking
    val latch = new CountDownLatch(1)

    frame.rows().subscribe(new Subscriber[Row]() {
      override def onError(e: Throwable) {
        logger.error("Error writing row", e)
        observer.onError(e)
      }

      override def onNext(row: Row) {
        if (row != null) {
          writer.write(row)
          observer.onNext(row)
          count.incrementAndGet()
        }
      }

      override def onCompleted() {
        writer.close()
        latch.countDown()
        observer.onCompleted()
      }
    })

    latch.await(1, TimeUnit.DAYS)
    count.get()
  }
}