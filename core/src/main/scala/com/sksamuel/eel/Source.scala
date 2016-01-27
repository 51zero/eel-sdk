package com.sksamuel.eel

import scala.language.implicitConversions

trait Source {
  def schema: FrameSchema = FrameSchema(loader.take(1).toList.head.columns)
  def loader: Iterator[Row]
}

object Source {
  implicit def toFrame(source: Source): Frame = Frame.fromSource(source)
}

