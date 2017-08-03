package io.eels.datastream

import java.util.concurrent.atomic.AtomicReference

import com.sksamuel.exts.Logging
import io.eels.{Chunk, Rec, Row}

class FindSubscriber(p: Rec => Boolean) extends Subscriber[Chunk] with Logging {

  val result = new AtomicReference[Either[Throwable, Option[Rec]]](null)

  private var subscription: Subscription = null
  private var value: Option[Rec] = None

  override def subscribed(c: Subscription): Unit = this.subscription = c

  override def error(t: Throwable): Unit = {
    logger.error("Subscriber received error", t)
    result.set(Left(t))
  }

  override def next(t: Chunk): Unit = {
    if (value.isEmpty) {
      value = t.find(p)
      if (value.isDefined) {
        logger.debug("Value found, cancelling rest of stream")
        subscription.cancel()
      }
    }
  }

  override def completed(): Unit = {
    result.set(Right(value))
  }
}
