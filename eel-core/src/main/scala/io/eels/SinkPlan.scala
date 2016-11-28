package io.eels

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.sksamuel.exts.Logging
import org.reactivestreams.{Subscriber, Subscription}

object SinkPlan extends Logging {

  def execute(sink: Sink, frame: Frame, listener: Listener): Long = {

    val schema = frame.schema()
    val writer = sink.writer(schema)
    var count = 0
    // the latch is just to make this execute method blocking
    val latch = new CountDownLatch(1)

    frame.rows().subscribe(new Subscriber[Row]() {

      var subscription: Subscription = _

      override def onSubscribe(s: Subscription): Unit = {
        this.subscription = s
        s.request(100)
      }

      override def onError(e: Throwable) {
        logger.error("Error writing row", e)
        listener.onError(e)
      }

      override def onNext(row: Row) {
        if (row != null) {

          writer.write(row)
          listener.onNext(row)

          count = count + 1
          if (count % 100 == 50)
            subscription.request(100)
        }
      }

      override def onComplete() {
        logger.debug("All data has been read from frame; sink will close writer(s) now")
        writer.close()
        latch.countDown()
        listener.onComplete()
        logger.info("Sink plan completed")
      }

    })

    latch.await(1, TimeUnit.DAYS)
    count
  }
}