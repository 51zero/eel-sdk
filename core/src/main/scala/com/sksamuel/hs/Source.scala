package com.sksamuel.hs

import com.sksamuel.hs.sink.Row

import scala.language.implicitConversions

trait Source {
  def loader: Iterator[Row]
}

object Source {
  implicit def toFrame(source: Source): Frame = Frame.fromSource(source)
}

