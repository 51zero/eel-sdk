package io.eels.datastream

trait Subscription {
  def cancel()
}

object Subscription {
  val empty = new Subscription {
    override def cancel(): Unit = ()
  }
}