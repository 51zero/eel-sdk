package com.sksamuel.eel

import com.sksamuel.eel.sink.Column

case class FrameSchema(columns: Seq[Column]) {
  def addColumn(col: Column): FrameSchema = copy(columns :+ col)
  def removeColumn(name: String): FrameSchema = copy(columns = columns.filterNot(_.name == name))
}