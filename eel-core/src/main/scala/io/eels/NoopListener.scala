package io.eels

object NoopListener extends Listener {
  override def onNext(row: Row): Unit = ()
}