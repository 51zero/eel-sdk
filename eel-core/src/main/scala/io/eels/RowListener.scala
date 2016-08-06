package io.eels

trait RowListener {
  def onRow(row: Row): Unit
}

object NoopRowListener extends RowListener {
  override def onRow(row: Row) = ()
}