package io.eels.datastream

import java.util.concurrent.atomic.AtomicBoolean

trait Subscription {
  def cancel()
}

object Subscription {

  val empty = new Subscription {
    override def cancel(): Unit = ()
  }

  def fromRunning(running: AtomicBoolean) = new Subscription {
    assert(running.get)
    override def cancel(): Unit = running.set(false)
  }
}