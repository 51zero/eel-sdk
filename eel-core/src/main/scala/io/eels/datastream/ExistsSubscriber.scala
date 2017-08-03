package io.eels.datastream

import java.util.concurrent.atomic.AtomicReference

import com.sksamuel.exts.Logging
import io.eels.{Chunk, Rec, Row}

class ExistsSubscriber(fn: Rec => Boolean) extends Subscriber[Chunk] with Logging {

  val result = new AtomicReference[Either[Throwable, Boolean]](null)

  private var subscription: Subscription = null
  private var exists = false

  override def subscribed(subscription: Subscription): Unit = this.subscription = subscription

  override def error(t: Throwable): Unit = {
    logger.error("Subscriber received error", t)
    result.set(Left(t))
  }

  override def next(t: Chunk): Unit = {
    if (!exists) {
      exists = t.exists(fn)
      if (exists) {
        logger.debug("Value found, cancelling rest of stream")
        subscription.cancel()
      }
    }
  }

  override def completed(): Unit = {
    result.set(Right(exists))
  }
}
