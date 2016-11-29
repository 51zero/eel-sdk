package io.eels.actions

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.function.Consumer

import io.eels.{Frame, Row}
import reactor.core.scheduler.Schedulers

object VectorAction extends Action {

  def apply(frame: Frame): Vector[Row] = {

    val builder = Vector.newBuilder[Row]
    var error: Throwable = null
    val latch = new CountDownLatch(1)

    frame.rows().subscribeOn(Schedulers.single()).subscribe(new Consumer[Row] {
      override def accept(row: Row): Unit = {
        builder.+=(row)
      }
    }, new Consumer[Throwable] {
      override def accept(t: Throwable): Unit = {
        error = t
        latch.countDown()
      }
    }, new Runnable {
      override def run(): Unit = latch.countDown()
    })

    latch.await(100, TimeUnit.DAYS)
    if (error != null)
      throw error
    builder.result()
  }
}


