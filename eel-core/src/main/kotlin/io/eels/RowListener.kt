package io.eels

interface RowListener {
  fun onRow(row: Row): Unit

  object Noop : RowListener {
    override fun onRow(row: Row) {
    }
  }
}