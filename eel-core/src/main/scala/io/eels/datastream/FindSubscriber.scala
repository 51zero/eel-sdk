package io.eels.datastream

import java.util.concurrent.atomic.AtomicReference

import com.sksamuel.exts.Logging
import io.eels.Row

class FindSubscriber(p: Row => Boolean) extends Subscriber[Seq[Row]] with Logging {

  val result = new AtomicReference[Either[Throwable, Option[Row]]](null)

  private var cancellable: Subscription = null
  private var value: Option[Row] = None

  override def subscribed(c: Subscription): Unit = this.cancellable = c

  override def error(t: Throwable): Unit = {
    logger.error("Subscriber received error", t)
    result.set(Left(t))
  }

  override def next(t: Seq[Row]): Unit = {
    if (value.isEmpty) {
      value = t.find(p)
      if (value.isDefined) {
        logger.debug("Value found, cancelling rest of stream")
        if (cancellable != null) {
          cancellable.cancel()
        }
      }
    }
  }

  override def completed(): Unit = {
    result.set(Right(value))
  }
}
