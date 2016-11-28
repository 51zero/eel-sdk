package io.eels

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.function.Consumer

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import reactor.core.scheduler.Schedulers

object ListPlan extends Plan {

  def apply(frame: Frame): List[Row] = {

    val builder = List.newBuilder[Row]
    var error: Throwable = null
    val latch = new CountDownLatch(1)

    frame.rows().publishOn(Schedulers.single(), requestSize).subscribe(new Consumer[Row] {
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

trait Plan extends Logging {
  val config = ConfigFactory.load()
  val requestSize = config.getInt("eel.execution.requestSize")
}
