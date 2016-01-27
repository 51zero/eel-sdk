package com.sksamuel.eel

import scala.language.implicitConversions

trait Source {

  def schema: FrameSchema = {
    val r = reader
    try {
      r.iterator.take(1).toList.headOption.fold(sys.error("No schema can be found")) { row => FrameSchema(row.columns) }
    } finally {
      r.close()
    }
  }

  def reader: Reader
}

object Source {
  implicit def toFrame(source: Source): Frame = Frame.fromSource(source)
}

trait Reader {
  def iterator: Iterator[Row]
  def close(): Unit
}
