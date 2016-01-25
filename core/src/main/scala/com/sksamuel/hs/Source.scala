package com.sksamuel.hs

import java.sql.DriverManager

import scala.language.implicitConversions

trait Source {
  def loader: Iterator[Seq[String]]
}

object Source {
  implicit def toFrame(source: Source): Frame = Frame.fromSource(source)
}

