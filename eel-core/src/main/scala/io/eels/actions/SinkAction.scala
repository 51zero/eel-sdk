package io.eels.actions

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.function.Consumer

import io.eels._
import reactor.core.scheduler.Schedulers

object SinkAction extends Action {

  def execute(sink: Sink, frame: Frame, listener: Listener): Long = {

    val schema = frame.schema
    val writer = sink.writer(schema)

    // the latch is just to make this execute method blocking
    val latch = new CountDownLatch(1)
    var error: Throwable = null
    var count = 0

    frame.rows().publishOn(Schedulers.single(), requestSize).subscribe(new Consumer[Row] {
      override def accept(row: Row): Unit = {
        writer.write(row)
        listener.onNext(row)
        count = count + 1
      }
    }, new Consumer[Throwable] {
      override def accept(t: Throwable): Unit = {
        error = t
        latch.countDown()
      }
    }, new Runnable {
      override def run(): Unit = {
        logger.debug("All data has been read from frame; sink will close writer(s) now")
        writer.close()
        latch.countDown()
        listener.onComplete()
      }
    })

    latch.await(1, TimeUnit.DAYS)
    logger.info("Sink plan completed")
    count
  }
}