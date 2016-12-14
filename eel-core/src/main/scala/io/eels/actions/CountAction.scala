package io.eels.actions

import io.eels.{Frame, Row}

object CountAction extends Action {
  def execute(frame: Frame): Long = frame.rows().size
}

object ForallAction extends Action {
  def execute(frame: Frame, p: Row => Boolean): Boolean = frame.rows().forall(p)
}

object ExistsAction extends Action {
  def execute(frame: Frame, p: Row => Boolean): Boolean = frame.rows().exists(p)
}

object FindAction extends Action {
  def execute(frame: Frame, p: Row => Boolean): Option[Row] = frame.rows().find(p)
}