package io.eels

class FoldPlan[A](a: A, fn: (A, Row) => A, frame: Frame) extends Plan[A] {
  override def run: A = frame.buffer.iterator.foldLeft(a)(fn)
}
