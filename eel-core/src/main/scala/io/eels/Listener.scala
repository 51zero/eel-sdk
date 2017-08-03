package io.eels

trait Listener {
  def started(): Unit = ()
  def onNext(row: Rec): Unit = ()
  def onError(t: Throwable): Unit = ()
  def onComplete(): Unit = ()
}
