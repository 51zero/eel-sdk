package com.sksamuel.eel

import com.sksamuel.eel.sink.Row

import scala.language.implicitConversions

trait Source {
  def schema: FrameSchema = FrameSchema(loader.take(1).toList.head.columns)
  def loader: Iterator[Row]
}

object Source {
  implicit def toFrame(source: Source): Frame = Frame.fromSource(source)
}

