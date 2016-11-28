package io.eels

trait Listener {
  def onNext(row: Row): Unit = ()
  def onError(t: Throwable): Unit = ()
  def onComplete(): Unit = ()
}
