package com.sksamuel.hs

import com.sksamuel.hs.sink.Row

trait Sink {
  def insert(row: Row)
  def completed(): Unit
}
