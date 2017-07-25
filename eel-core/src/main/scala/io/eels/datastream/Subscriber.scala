package io.eels.datastream

trait Subscriber[T] {
  def subscribed(subscription: Subscription)
  def next(t: T)
  def completed()
  def error(t: Throwable)
}
