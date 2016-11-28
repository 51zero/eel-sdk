package io.eels

import java.util.concurrent.BlockingQueue

import org.reactivestreams.{Publisher, Subscriber, Subscription}

class BlockingQueuePublisher[T](queue: BlockingQueue[T], sentinel: T) extends Publisher[T] {

  override def subscribe(s: Subscriber[_ >: T]): Unit = {

    s.onSubscribe(new Subscription {

      var completed = false

      override def cancel(): Unit = {
        if (!completed) {
          completed = true
          s.onComplete()
        }
      }

      override def request(n: Long): Unit = {
        if (!completed) {
          var remaining = n
          while (remaining > 0 && !completed) {
            remaining = remaining - 1
            val t = queue.take()
            if (t == sentinel) {
              queue.put(sentinel)
              s.onComplete()
              completed = true
            } else {
              s.onNext(t)
            }
          }
        }
      }
    })
  }
}
