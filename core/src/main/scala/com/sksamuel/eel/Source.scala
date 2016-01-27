package com.sksamuel.eel

import com.sksamuel.eel.sink.Row

import scala.language.implicitConversions

trait Source {
  def loader: Iterator[Row]
}

object Source {
  implicit def toFrame(source: Source): Frame = Frame.fromSource(source)
}

