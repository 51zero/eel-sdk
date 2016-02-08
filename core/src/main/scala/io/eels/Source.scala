package io.eels

import scala.language.implicitConversions

trait Source {
  def schema: FrameSchema
  def parts: Seq[Part]
}

object Source {
  implicit def toFrame(source: Source): Frame = Frame.fromSource(source)
}