package io.eels.datastream

class DelegateSubscriber[T](delegate: Subscriber[T]) extends Subscriber[T] {
  override def subscribed(c: Subscription): Unit = delegate.subscribed(c)
  override def completed(): Unit = delegate.completed()
  override def error(t: Throwable): Unit = delegate.error(t)
  override def next(t: T): Unit = delegate.next(t)
}
