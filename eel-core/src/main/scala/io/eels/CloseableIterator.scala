package io.eels

trait CloseableIterator[+T] {
  outer =>

  private def iterator = new Iterator[T] {
    override def hasNext: Boolean = outer.hasNext()
    override def next(): T = outer.next()
  }

  def foreach(fn: T => Any): Unit = {
    iterator.takeWhile(_ => !isClosed()).foreach(fn)
  }

  def hasNext(): Boolean
  def next(): T

  def close(): Unit
  def isClosed(): Boolean
}