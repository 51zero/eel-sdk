package io.eels.datastream

import java.util.concurrent.atomic.AtomicBoolean

import com.sksamuel.exts.Logging

trait Subscription {
  def cancel()
}

object Subscription {

  val empty = new Subscription {
    override def cancel(): Unit = ()
  }

  def fromRunning(running: AtomicBoolean) = new Subscription with Logging {
    assert(running.get)
    override def cancel(): Unit = {
      logger.debug(s"Subscription cancellation requested")
      running.set(false)
    }
  }
}