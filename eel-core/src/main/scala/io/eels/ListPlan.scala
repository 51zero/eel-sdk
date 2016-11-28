package io.eels

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.reactivestreams.{Subscriber, Subscription}

object ListPlan {

  val RequestSize = 1000


  def apply(frame: Frame): List[Row] = {

    var error: Throwable = null
    var count = 0
    var subscription: Subscription = null
    val latch = new CountDownLatch(1)
    val builder = List.newBuilder[Row]

    frame.rows().subscribe(new Subscriber[Row] {

      override def onSubscribe(s: Subscription): Unit = {
        subscription = s
        s.request(RequestSize)
      }

      override def onError(t: Throwable): Unit = {
        error = t
        latch.countDown()
      }

      override def onComplete(): Unit = latch.countDown()

      override def onNext(row: Row): Unit = {
        builder.+=(row)
        count = count + 1
        if (count % RequestSize == RequestSize / 2)
          subscription.request(RequestSize)
      }
    })

    latch.await(100, TimeUnit.DAYS)
    if (error != null)
      throw error
    builder.result()
  }
}
