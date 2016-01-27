package com.sksamuel.eel

import com.sksamuel.eel.sink.Row

trait Sink {
  def insert(row: Row)
  def completed(): Unit
}
