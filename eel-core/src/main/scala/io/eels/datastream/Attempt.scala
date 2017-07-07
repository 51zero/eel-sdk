package io.eels.datastream

object Attempt {
  def apply[T](subscriber: Subscriber[T])(fn: Subscriber[T] => Unit): Unit = try {
    fn(subscriber)
    subscriber.completed()
  } catch {
    case t: Throwable => subscriber.error(t)
  }
}
