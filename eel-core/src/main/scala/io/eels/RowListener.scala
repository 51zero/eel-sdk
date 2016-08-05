package io.eels

trait RowListener {
  def onRow(row: Row): Unit

  object Noop extends RowListener {
    override def onRow(row: Row) = ()
  }
}